package com.snowplowanalytics.snowflake.core

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

object BadRowSchemas {
  val LoaderFailureWithUnknownData = SchemaKey("com.snowplowanalytics.snowplow.badrows", "loader_parsing_error", "jsonschema", SchemaVer.Full(1, 0, 0))
  val SnowflakeSpecificFailure = SchemaKey("com.snowplowanalytics.snowplow.badrows", "snowflake_error", "jsonschema", SchemaVer.Full(1, 0, 0))
}
