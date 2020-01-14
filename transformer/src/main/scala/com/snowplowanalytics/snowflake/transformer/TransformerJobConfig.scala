/*
 * Copyright (c) 2017-2020 Snowplow Analytics Ltd. All rights reserved.
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

import com.snowplowanalytics.snowflake.core.Config._

sealed trait TransformerJobConfig {
  def input: String
  def goodOutput: String
  def badOutput: Option[String]
  def runId: String
}

object TransformerJobConfig {

  case class S3Config(enrichedArchive: S3Folder, snowflakeOutput: S3Folder, badOutputFolder: Option[S3Folder], runId: String) extends TransformerJobConfig {
    def input: String = {
      val (enrichedBucket, enrichedPath) = enrichedArchive.splitS3Folder
      s"s3a://$enrichedBucket/$enrichedPath$runIdFolder/*"
    }

    def goodOutput: String = {
      val (bucket, path) = snowflakeOutput.splitS3Folder
      s"s3a://$bucket/$path$runIdFolder"
    }

    def badOutput: Option[String] = {
      badOutputFolder.map { o =>
        val (bucket, path) = o.splitS3Folder
        s"s3a://$bucket/$path$runIdFolder"
      }
    }

    def runIdFolder: String = runId.split("/").last
  }

  case class FSConfig(input: String, goodOutput: String, badOutput: Option[String]) extends TransformerJobConfig {
    def runId: String = "fs-run-id"
  }
}
