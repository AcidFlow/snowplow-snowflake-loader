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
package com.snowplowanalytics.snowflake.loader.ast

import cats.Show
import cats.syntax.show._

case class Column(
  name: String,
  dataType: SnowflakeDatatype,
  notNull: Boolean = false,
  unique: Boolean = false)

object Column {

  implicit object ColumnShow extends Show[Column] {
    def show(ddl: Column): String = {
      val datatype = ddl.dataType.show
      val constraints = ((if (ddl.notNull) "NOT NULL" else "") :: (if (ddl.unique) "UNIQUE" else "") :: Nil).filterNot(_.isEmpty)
      val renderedConstraints = if (constraints.isEmpty) "" else " " + constraints.mkString(" ")
      s"${ddl.name} $datatype" + renderedConstraints
    }
  }
}
