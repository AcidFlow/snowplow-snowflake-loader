/*
 * Copyright (c) 2019 Snowplow Analytics Ltd. All rights reserved.
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

import cats.data.NonEmptyList
import com.snowplowanalytics.iglu.core.SelfDescribingData
import io.circe.{Encoder, Json}
import io.circe.syntax._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowflake.core.BadRowSchemas._
import com.snowplowanalytics.snowflake.generated.ProjectMetadata
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._

/**
  * Represents the rows which are failing to transformed appropriate format
  */
sealed trait BadRow {
  def getSelfDescribingData: SelfDescribingData[Json]
  def toCompactJson: String  = getSelfDescribingData.asJson.noSpaces
}

object BadRow {

  private val SnowflakeProcessorInfo = Processor(ProjectMetadata.name, ProjectMetadata.version)

  /**
    * Represents the failure case where data can not be parsed as a proper event
    * @param payload data blob tried to be parsed
    * @param errors errors in the end of the parsing process
    */
  final case class ParsingFailure(payload: String, errors: NonEmptyList[String]) extends BadRow {
    def getSelfDescribingData: SelfDescribingData[Json] =
      SelfDescribingData(LoaderFailureWithUnknownData, (this: BadRow).asJson)
  }

  /**
    * Represents the Snowflake specific failure cases
    * @param enrichedEvent event which is enriched successfully
    * @param failures info of failures
    */
  final case class SnowflakeFailure(enrichedEvent: Event, failures: NonEmptyList[Failure]) extends BadRow {
    def getSelfDescribingData: SelfDescribingData[Json] =
      SelfDescribingData(SnowflakeSpecificFailure, (this: BadRow).asJson)
  }

  implicit val badRowCirceJsonEncoder: Encoder[BadRow] =
    Encoder.instance {
      case ParsingFailure(original, errors) =>
        Json.obj(
          "payload" := original.asJson,
          "errors" := errors.asJson,
          "processor" := SnowflakeProcessorInfo.asJson
        )
      case SnowflakeFailure(original, failures) =>
        Json.obj(
          "enrichedEvent" := original.asJson,
          "failures" := failures.asJson,
          "processor" := SnowflakeProcessorInfo.asJson
        )
    }

  /**
    * Gives info about the reasons of the Snowflake specific failures
    */
  sealed trait Failure

  object Failure {

    /**
      * Represents Snowflake specific failure due to data of the one of the fields of the enriched event
      * @param value problematic value
      * @param column column where failure occurred
      * @param message error message
      */
    final case class ValueError(value: Json, column: String, message: String) extends Failure


    /**
      * Represents deduplication error
      * @param message error message
      */
    final case class DeduplicationError(message: String) extends Failure

    implicit val failureInfoCirceJsonEncoder: Encoder[Failure] =
      Encoder.instance {
        case ValueError(value, column, message) =>
          Json.obj("valueError" :=
            Json.obj(
              "value" := value,
              "column" := column.asJson,
              "message" := message.asJson
            )
          )

        case DeduplicationError(message) =>
          Json.obj("deduplicationError" := Json.obj("message" := message.asJson))
      }
  }

  /**
    * Gives info about the processor where error occurred
    * @param artifact
    * @param version
    */
  final case class Processor(artifact: String, version: String)

  implicit val processorCirceJsonEncoder: Encoder[Processor] =
    Encoder.instance {
      case Processor(artifact, version) =>
        Json.obj(
          "artifact" := artifact.asJson,
          "version" := version.asJson
        )
    }
}
