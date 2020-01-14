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
package com.snowplowanalytics.snowflake.loader.ast

import org.specs2.mutable.Specification

import scala.io.Source

import com.snowplowanalytics.snowflake.loader.ast.Statement._

class AtomicDefSpec extends Specification {
  "getTable" should {
    "CREATE atomic.events" in {
      val referenceStream = getClass.getResourceAsStream("/sql/atomic-def.sql")
      val expectedLines = Source.fromInputStream(referenceStream).getLines().toList
      val expected = List(AtomicDefSpec.normalizeSql(expectedLines).mkString(""))

      val resultLines = AtomicDef.getTable().getStatement.value.split("\n").toList
      val result = AtomicDefSpec.normalizeSql(resultLines)

      result must beEqualTo(expected)
    }
  }
}


object AtomicDefSpec {
  /** Remove comments and formatting */
  def normalizeSql(lines: List[String]) = lines
    .map(_.dropWhile(_.isSpaceChar))
    .map(line => if (line.startsWith("--")) "" else line)
    .filterNot(_.isEmpty)
    .map(_.replaceAll("""\s+""", " "))
    .map(_.replaceAll(""",\s""", ","))
}
