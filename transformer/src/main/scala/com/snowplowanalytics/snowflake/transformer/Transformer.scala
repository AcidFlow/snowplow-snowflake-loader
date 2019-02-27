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

// java
import java.util.UUID

// circe
import io.circe.Json
import io.circe.syntax._

// cats
import cats.data.Validated.{Invalid, Valid}

// schema-ddl
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema

// scala-analytics-sdk
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, SnowplowEvent}

// events-manifest
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest.EventsManifestConfig

// This library
import com.snowplowanalytics.snowflake.transformer.singleton.EventsManifestSingleton

object Transformer {

  /**
    * Transform jsonified TSV to pair of shredded keys and enriched event in JSON format
    *
    * @param event                Event case class instance
    * @param eventsManifestConfig events manifest config instance
    * @return pair of set with column names and JValue
    */
  def transform(event: Event, eventsManifestConfig: Option[EventsManifestConfig], atomicSchema: Schema): Option[(Set[String], String)] = {
    val shredTypes = event.inventory.map(item => SnowplowEvent.transformSchema(item.shredProperty, item.schemaKey))
    val eventId = event.event_id.toString
    val eventFingerprint = event.event_fingerprint.getOrElse(UUID.randomUUID().toString)
    val etlTstamp = event.etl_tstamp.map(i => EventsManifest.RedshiftTstampFormatter.format(i))
      .getOrElse(throw new RuntimeException(s"etl_tstamp in event $eventId is empty or missing"))
    val atomic = atomicSchema.properties.map { properties => properties.value.mapValues { property =>
      property.maxLength.map {_.value.intValue}
    }}.getOrElse(throw new RuntimeException(s"Could not convert atomic schema to property map"))

    EventsManifestSingleton.get(eventsManifestConfig) match {
      case Some(manifest) =>
        if (manifest.put(eventId, eventFingerprint, etlTstamp)) {
          Some((shredTypes, truncateFields(event.toJson(true), atomic).noSpaces))
        } else None
      case None => Some((shredTypes, truncateFields(event.toJson(true), atomic).noSpaces))
    }
  }

  /**
    * Transform TSV to pair of inventory items and JSON object
    *
    * @param line enriched event TSV
    * @return Event case class instance
    */
  def jsonify(line: String): Event = {
    Event.parse(line) match {
      case Valid(event) => event
      case Invalid(e) => throw new RuntimeException(e.toList.mkString("\n"))
    }
  }

  /**
    * Truncate a Snowplow event's fields based on atomic schema
    */
  def truncateFields(eventJson: Json, atomic: Map[String, Option[Int]]): Json = {
    Json.fromFields(eventJson.asObject.getOrElse(throw new RuntimeException(s"Event JSON is not an object? $eventJson")).toList.map {
      case (key, value) if value.isString =>
        atomic.get(key) match {
          case Some(Some(length)) => (key, value.asString.map { s =>
            (if (s.length > length) s.take(length) else s).asJson
          }.getOrElse(value))
          case _ => (key, value)
        }
      case other => other
    })
  }
}
