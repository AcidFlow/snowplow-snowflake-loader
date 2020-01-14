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

import cats.Show
import cats.syntax.show._

import Select._

/**
  * AST for SELECT statement
  * E.g. SELECT raw:app_id::VARCHAR, event_id:event_id::VARCHAR FROM temp_table
  * @param columns list of columns, casted to specific type
  * @param table source table name
  */
case class Select(columns: List[CastedColumn], schema: String, table: String)

object Select {
  case class CastedColumn(originColumn: String, columnName: String, datatype: SnowflakeDatatype, substring: Option[Substring], tryCast: Boolean)
  case class Substring(start: Int, length: Int)

  implicit val castedColumnShow: Show[Select.CastedColumn] =
    new Show[Select.CastedColumn] {
      def show(column: Select.CastedColumn): String = {
        val castedColumn = if (column.tryCast) s"TRY_CAST(${column.originColumn}:${column.columnName} as ${column.datatype.show})"
        else s"${column.originColumn}:${column.columnName}::${column.datatype.show}"
        column.substring match {
          case Some(Substring(start, length)) => s"substr($castedColumn,$start,$length)"
          case None => castedColumn
        }
      }
    }
}
