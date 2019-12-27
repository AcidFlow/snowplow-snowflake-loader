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
package com.snowplowanalytics.snowflake.loader

import  com.snowplowanalytics.snowflake.core.Cli

object Main {
  def main(args: Array[String]): Unit = {
    Cli.parse(args) match {
      case Right(Cli.Loader.Load(config, dryRun)) =>
        println("Loading...")
        Loader.run(config, dryRun)
      case Right(Cli.Loader.Setup(config, skip, _)) =>
        println("Setting up...")
        Initializer.run(config, skip)
      case Right(Cli.Loader.Migrate(config, version, _)) =>
        println("Migrating...")
        Migrator.run(config, version)
      case Left(error) =>
        System.err.println(error)
        sys.exit(1)
    }
  }
}
