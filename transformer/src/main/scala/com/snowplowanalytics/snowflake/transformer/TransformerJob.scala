/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowflake.transformer

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import cats.syntax.either._

import io.circe.syntax._

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.snowflake.core.ProcessManifest
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest.EventsManifestConfig
import com.snowplowanalytics.snowflake.transformer.singleton.EventsManifestSingleton

object TransformerJob {

  /** Process all directories, saving state into DynamoDB */
  def run(spark: SparkSession, manifest: ProcessManifest, tableName: String, jobConfigs: List[TransformerJobConfig], eventsManifestConfig: Option[EventsManifestConfig], inbatch: Boolean, atomicSchema: Schema): Unit = {
    jobConfigs.foreach { jobConfig =>
      println(s"Snowflake Transformer: processing ${jobConfig.runId}. ${System.currentTimeMillis()}")
      manifest.add(tableName, jobConfig.runId)
      val shredTypes = process(spark, jobConfig, eventsManifestConfig, inbatch, atomicSchema)
      manifest.markProcessed(tableName, jobConfig.runId, shredTypes, jobConfig.goodOutput)
      println(s"Snowflake Transformer: processed ${jobConfig.runId}. ${System.currentTimeMillis()}")
    }
  }

  /**
    * Transform particular folder to Snowflake-compatible format and
    * return list of discovered shredded types
    *
    * @param spark                Spark SQL session
    * @param jobConfig            configuration with paths
    * @param eventsManifestConfig events manifest config instance
    * @param inbatch              whether inbatch deduplication should be used
    * @param atomicSchema         map of field names to maximum lengths
    * @return list of discovered shredded types
    */
  def process(spark: SparkSession, jobConfig: TransformerJobConfig, eventsManifestConfig: Option[EventsManifestConfig], inbatch: Boolean, atomicSchema: Schema) = {
    jobConfig.badOutput.fold(
      processWithoutBadRow(spark, jobConfig.input, jobConfig.goodOutput, eventsManifestConfig, inbatch, atomicSchema)
    )(
      badOutput => processWithBadRow(spark, jobConfig.input, jobConfig.goodOutput, badOutput, eventsManifestConfig, inbatch, atomicSchema)
    )
  }

  def processWithBadRow(spark: SparkSession, input: String, goodOutput: String, badOutput: String, eventsManifestConfig: Option[EventsManifestConfig], inbatch: Boolean, atomicSchema: Schema) = {
    import spark.implicits._

    val sc = spark.sparkContext
    val keysAggregator = new StringSetAccumulator
    sc.register(keysAggregator)

    val inputRDD = sc
      .textFile(input)
      .map { e => Transformer.jsonify(e) }
      .cache()

    // Handling of malformed rows; drop good, turn malformed into `BadRow`
    val withParsingFailure = inputRDD
      .flatMap { shredded => shredded.swap.toOption }
      .map { badRow => Row(badRow.toCompactJson) }

    // Taking good rows
    val withoutParsingFailure = inputRDD.flatMap(_.toOption)

    // Checking Snowflake specific error cases
    val snowflakeErrorCheckRes = withoutParsingFailure
      .map { e => SnowflakeErrorCheck(e).toLeft(e) }
      .cache()

    // Handling of rows which can not pass Snowflake specific error check;
    // drop good, turn rows who can not pass the check into `BadRow`
    val withSnowflakeSpecificErrors = snowflakeErrorCheckRes
      .flatMap { e => e.swap.toOption }
      .map { badRow => Row(badRow.toCompactJson) }

    // Taking good rows in the result of Snowflake error check
    val withoutSnowflakeSpecificErrors = snowflakeErrorCheckRes.flatMap(_.toOption)

    // Deduplicate the events in a batch if inbatch flagged set
    val inBatchDedupedEvents = if (inbatch) {
      withoutSnowflakeSpecificErrors
        .groupBy { e => (e.event_id, e.event_fingerprint) }
        .flatMap { case (_, vs) => vs.take(1) }
    } else withoutSnowflakeSpecificErrors

    // Perform cross-batch natural deduplications
    val crossBatchDedupedEvents = inBatchDedupedEvents
      .map { e => (e, Transformer.dedupeCrossBatch(e, EventsManifestSingleton.get(eventsManifestConfig))) }
      .cache()

    // Deduplication operation succeeded
    val dupeSucceeded = crossBatchDedupedEvents
      .filter {
        case (_, Right(r)) => r
        case (_, Left(_)) => false
      }

    // Deduplication operation failed due to DynamoDB
    val dupeFailed = crossBatchDedupedEvents.flatMap {
      case (_, Left(m)) => Some(Row(m.toCompactJson))
      case _ => None
    }

    // Transform events and add shredded keys to aggregator
    val transformedEvents = dupeSucceeded.flatMap { case(e, _) =>
      Transformer.transform(e, atomicSchema) match {
        case Some((keys, transformed)) =>
          keysAggregator.add(keys)
          Some(transformed)
        case None => None
      }
    }

    // DataFrame is used only for S3OutputFormat
    transformedEvents.toDF.write.mode(SaveMode.Append).text(goodOutput)

    spark.createDataFrame(withParsingFailure ++ withSnowflakeSpecificErrors ++ dupeFailed, StructType(StructField("_", StringType, true) :: Nil))
        .write
        .mode(SaveMode.Overwrite)
        .text(badOutput)

    val keysFinal = keysAggregator.value.toList
    keysAggregator.reset()
    keysFinal
  }

  def processWithoutBadRow(spark: SparkSession, input: String, output: String, eventsManifestConfig: Option[EventsManifestConfig], inbatch: Boolean, atomicSchema: Schema) = {
    import spark.implicits._

    val sc = spark.sparkContext
    val keysAggregator = new StringSetAccumulator
    sc.register(keysAggregator)

    val inputRDD = sc
      .textFile(input)
      .map { e => Transformer.jsonify(e) }
      .flatMap {
        shredded => shredded.leftMap(e => throw new RuntimeException(e.toCompactJson)).toOption
      }
      .flatMap {
        event => SnowflakeErrorCheck(event).toLeft(event).leftMap(error => throw new RuntimeException(error.toCompactJson)).toOption
      }

    // Deduplicate the events in a batch if inbatch flagged set
    val inBatchDedupedEvents = if (inbatch) {
      inputRDD
        .groupBy { e => (e.event_id, e.event_fingerprint) }
        .flatMap { case (_, vs) => vs.take(1) }
    } else inputRDD

    // Perform cross-batch natural deduplications
    val transformedEvents = inBatchDedupedEvents
      .map { e => (e, Transformer.dedupeCrossBatch(e, EventsManifestSingleton.get(eventsManifestConfig))) }
      .filter {
        case (_, Right(r)) => r
        case (_, Left(e)) => throw new RuntimeException(e.toCompactJson)
      }
      .flatMap { case(e, _) =>
        Transformer.transform(e, atomicSchema) match {
          case Some((keys, transformed)) =>
            keysAggregator.add(keys)
            Some(transformed)
          case None => None
        }
      }

    // DataFrame is used only for S3OutputFormat
    transformedEvents.toDF.write.mode(SaveMode.Append).text(output)

    val keysFinal = keysAggregator.value.toList
    keysAggregator.reset()
    keysFinal
  }
}
