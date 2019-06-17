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

import java.time.Instant

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import cats.syntax.either._
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest.EventsManifestConfig
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowflake.core.ProcessManifest
import com.snowplowanalytics.snowflake.transformer.singleton.EventsManifestSingleton

object TransformerJob {

  private[transformer] val classesToRegister: Array[Class[_]] = Array(
    classOf[Array[String]],
    classOf[SchemaKey],
    classOf[SelfDescribingData[_]],
    classOf[Event],
    classOf[Instant],
    classOf[com.snowplowanalytics.iglu.core.SchemaVer$Full],
    classOf[io.circe.JsonObject$LinkedHashMapJsonObject],
    classOf[io.circe.Json$JObject],
    classOf[io.circe.Json$JString],
    classOf[io.circe.Json$JArray],
    classOf[io.circe.Json$JNull$],
    classOf[io.circe.Json$JNumber],
    classOf[io.circe.Json$JBoolean],
    classOf[io.circe.Json],
    Class.forName("io.circe.JsonLong"),
    Class.forName("io.circe.JsonDecimal"),
    Class.forName("io.circe.JsonBigDecimal"),
    Class.forName("io.circe.JsonBiggerDecimal"),
    Class.forName("io.circe.JsonDouble"),
    Class.forName("io.circe.JsonFloat"),
    classOf[java.util.LinkedHashMap[_, _]],
    classOf[java.util.ArrayList[_]],
    classOf[scala.collection.immutable.Map$EmptyMap$],
    classOf[scala.collection.immutable.Set$EmptySet$],
    classOf[org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage],
    classOf[org.apache.spark.sql.execution.datasources.FileFormatWriter$WriteTaskResult]
  )

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
    import spark.implicits._

    // Decide whether bad rows will be stored or not
    // If badOutput is supplied in the config, bad rows
    // need to be stored to given URL
    val badRowStored: Boolean = jobConfig.badOutput.isDefined

    val sc = spark.sparkContext
    val keysAggregator = new StringSetAccumulator
    sc.register(keysAggregator)

    val inputRDD = sc
      .textFile(jobConfig.input)
      .map(l => applyOpsWithPotentialError(l, eventsManifestConfig))
      .cache()

    // Check if bad rows should be stored in case of error.
    // If bad rows need to be stored continue execution,
    // if bad rows not need to be stored, throw exception
    val withoutError = inputRDD
      .flatMap {
        _.leftMap { e =>
          if (badRowStored) e else throw new RuntimeException(e.toCompactJson)
        }.toOption
      }
      .flatMap(e => e)

    // Deduplicate the events in a batch if inbatch flagged set
    val inBatchDedupedEvents = if (inbatch) {
      withoutError
        .groupBy { e => (e.event_id, e.event_fingerprint) }
        .flatMap { case (_, vs) => vs.take(1) }
    } else withoutError

    val transformedEvents = inBatchDedupedEvents.map { e =>
      Transformer.transform(e, atomicSchema) match {
        case (keys, transformed) =>
          keysAggregator.add(keys)
          transformed
      }
    }

    // DataFrame is used only for S3OutputFormat
    transformedEvents
      .toDF
      .write
      .mode(SaveMode.Append)
      .text(jobConfig.goodOutput)

    jobConfig.badOutput.foreach { badOutput =>
      val withError = inputRDD
        .flatMap(_.swap.toOption)
        .map(e => Row(e.toCompactJson))
      spark.createDataFrame(withError, StructType(StructField("_", StringType, true) :: Nil))
        .write
        .mode(SaveMode.Overwrite)
        .text(badOutput)
    }

    val keysFinal = keysAggregator.value.toList
    keysAggregator.reset()
    keysFinal
  }

  /**
    * Apply operations which can output bad row
    * @param line line to process
    * @param eventsManifestConfig events manifest config instance
    * @return Left(BadRow) in case of error in one of the operations or
    *         Right(Some(event)) in case of event is not duplicated with another one
    *         in the another batch or Right(None) if event is duplicated.
    */
  private def applyOpsWithPotentialError(line: String,  eventsManifestConfig: Option[EventsManifestConfig]): Either[BadRow, Option[Event]] = {
    for {
      event <- Transformer.jsonify(line)
      sfErrChecked <- SnowflakeErrorCheck(event).toLeft(event)
      crossBatchDeduped <- Transformer.dedupeCrossBatch(sfErrChecked, EventsManifestSingleton.get(eventsManifestConfig))
        .map(t => if (t) Some(sfErrChecked) else None)
    } yield crossBatchDeduped
  }
}
