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
package com.snowplowanalytics.snowflake.transformer.bad

import cats.data.NonEmptyList
import io.circe.syntax._
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.validation.ValidatableJValue.validate
import com.snowplowanalytics.snowflake.transformer.BadRow
import com.snowplowanalytics.snowflake.transformer.BadRow.Failure.{DeduplicationError, ValueError}
import com.snowplowanalytics.snowflake.transformer.BadRow.{ParsingFailure, SnowflakeFailure}
import com.snowplowanalytics.snowflake.transformer.good.TransformerSpec
import org.json4s.jackson.JsonMethods.parse
import org.scalacheck.Gen
import org.scalacheck.Prop.forAll
import org.specs2.{ScalaCheck, Specification}

object BadRowSchemaValidationSpec {

  val resolverConfig =
    """
      |{
      |   "schema":"iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
      |   "data":{
      |      "cacheSize":500,
      |      "repositories":[
      |         {
      |            "name":"Iglu Central",
      |            "priority":0,
      |            "vendorPrefixes":[
      |               "com.snowplowanalytics"
      |            ],
      |            "connection":{
      |               "http":{
      |                  "uri":"http://iglucentral.com"
      |               }
      |            }
      |         }
      |      ]
      |   }
      |}
      |
    """.stripMargin

  val resolver = Resolver.parse(parse(resolverConfig)).valueOr(e => throw new RuntimeException(s"Test resolver is invalid: $e"))

  val parsingFailureGen = for {
    payload <- Gen.identifier
    errors <- Gen.nonEmptyListOf(Gen.identifier)
  } yield ParsingFailure(payload, NonEmptyList.fromListUnsafe(errors))

  val valueErrorGen = for {
    column <- Gen.identifier
    message <- Gen.identifier
  }  yield ValueError("mockJsonValue".asJson, column, message)

  val deduplicationErrorGen = for {
    message <- Gen.identifier
  } yield DeduplicationError(message)

  val snowflakeFailureGen = for {
    failureGen <- Gen.oneOf(valueErrorGen, deduplicationErrorGen)
    failureList <- Gen.nonEmptyListOf(failureGen)
  } yield SnowflakeFailure(TransformerSpec.event, NonEmptyList.fromListUnsafe(failureList))

  def validateBadRow(badRow: BadRow) =
    validate(parse(badRow.toCompactJson), dataOnly = true)(resolver).toEither
}

class BadRowSchemaValidationSpec extends Specification with ScalaCheck { def is = s2"""
  self describing json of 'parsing failure' complies its schema $e1
  self describing json of 'snowflake failure' complies its sxhema $e2
  """

  def e1 = {
    forAll(BadRowSchemaValidationSpec.parsingFailureGen) {
      parsingFailure => BadRowSchemaValidationSpec.validateBadRow(parsingFailure) must beRight
    }
  }

  def e2 = {
    forAll(BadRowSchemaValidationSpec.snowflakeFailureGen) {
      snowflakeFailure => BadRowSchemaValidationSpec.validateBadRow(snowflakeFailure) must beRight
    }
  }
}