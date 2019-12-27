/*
 * Copyright (c) 2017-2019 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowflake.core

import java.nio.file.{Files, InvalidPathException, Path, Paths}
import java.util.Base64

import scala.concurrent.duration.TimeUnit

import cats.Id
import cats.data.{EitherT, ValidatedNel}
import cats.implicits._
import cats.effect.Clock

import io.circe.Json
import io.circe.syntax._
import io.circe.parser.{parse => jsonParse}

import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._

sealed trait Cli

object Cli {
  import Config._

  type PathOrJson = Either[Path, Json]

  object PathOrJson {
    def parse(t: (String, Boolean)): ValidatedNel[String, PathOrJson] = t match {
      case (string, encoded) =>
        val result = if (encoded)
          Either
            .catchOnly[IllegalArgumentException](new String(Base64.getDecoder.decode(string)))
            .leftMap(_.getMessage)
            .flatMap(s => jsonParse(s).leftMap(_.show))
            .map(_.asRight)
        else Either.catchOnly[InvalidPathException](Paths.get(string).asLeft).leftMap(_.getMessage)
        result
          .leftMap(error => s"Cannot parse as ${if (encoded) "base64-encoded JSON" else "FS path"}: $error")
          .toValidatedNel
    }

    def load(value: PathOrJson): EitherT[Id, String, Json] =
      value match {
        case Right(json) =>
          EitherT.rightT[Id, String](json)
        case Left(path) =>
          Either
            .catchNonFatal(new String(Files.readAllBytes(path)))
            .leftMap(e => s"Cannot read the path: ${e.getMessage}")
            .flatMap(s => jsonParse(s).leftMap(_.show))
            .toEitherT[Id]
      }
  }

  case class Transformer(loaderConfig: PathOrJson,
                         resolver: PathOrJson,
                         eventsManifestConfig: Option[PathOrJson],
                         inbatch: Boolean)

  sealed trait Loader extends Cli
  object Loader {

    sealed trait RawCli extends Product with Serializable {
      def loaderConfig: PathOrJson
      def resolver: PathOrJson
    }

    final case class LoadRaw(loaderConfig: PathOrJson, resolver: PathOrJson, dryRun: Boolean) extends RawCli
    final case class SetupRaw(loaderConfig: PathOrJson, resolver: PathOrJson, skip: Set[SetupSteps], dryRun: Boolean) extends RawCli
    final case class MigrateRaw(loaderConfig: PathOrJson, resolver: PathOrJson, loaderVersion: String, dryRun: Boolean) extends RawCli

    final case class Load(loaderConfig: Config, dryRun: Boolean) extends Loader
    final case class Setup(loaderConfig: Config, skip: Set[SetupSteps], dryRun: Boolean) extends Loader
    final case class Migrate(loaderConfig: Config, loaderVersion: String, dryRun: Boolean) extends Loader

    implicit val idClock: cats.effect.Clock[Id] = new Clock[Id] {
      def realTime(unit: TimeUnit): Id[Long] =
        unit.convert(System.currentTimeMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
      def monotonic(unit: TimeUnit): Id[Long] =
        unit.convert(System.nanoTime(), java.util.concurrent.TimeUnit.NANOSECONDS)
    }

    def init(rawCli: RawCli): Either[String, Loader] = {
      val result = for {
        resolverJson <- PathOrJson.load(rawCli.resolver)
        igluClient <- Client.parseDefault[Id](resolverJson).leftMap(_.show)
        configJson <- PathOrJson.load(rawCli.loaderConfig)
        configData <- SelfDescribingData.parse(configJson).leftMap(e => s"Configuration JSON is not self-describing, ${e.code}").toEitherT[Id]
        _ <- igluClient.check(configData).leftMap(e => s"Iglu validation failed with following error\n: ${e.asJson.spaces2}")
        cfg <- configData.data.as[Config].toEitherT[Id].leftMap(_.show)
      } yield rawCli match {
        case LoadRaw(_, _, dryRun) => Load(cfg, dryRun)
        case SetupRaw(_, _, skip, dryRun) => Setup(cfg, skip, dryRun)
        case MigrateRaw(_, _, version, dryRun) => Migrate(cfg, version, dryRun)
      }

      result.value
    }
  }

  import com.monovore.decline.{ Opts, Command, Argument }

  implicit val stepOpt = new Argument[SetupSteps] {
    def read(string: String): ValidatedNel[String, SetupSteps] =
      SetupSteps
        .withNameInsensitiveOption(string)
        .toValidNel(s"Step $string is unknown. Available options: ${SetupSteps.allStrings}")

    override def defaultMetavar: String = "step"
  }

  val dryRun = Opts.flag("dry-run", "Do not perform database actions, only print statements to stdout").orFalse
  val base64 = Opts.flag("base64", "Configuration passed as Base64-encoded string, not as file path").orFalse
  val version = Opts.option[String]("loader-version", s"Snowplow Snowflake Loader version to make the table compatible with")
  val steps = Opts.options[SetupSteps]("skip", s"Skip the setup step. Available steps: ${Config.SetupSteps.values}").orNone.map(_.toList.unite.toSet)

  val resolverOpt = Opts.option[String]("resolver", "Iglu Resolver JSON config, FS path or base64-encoded")
  val resolver = (resolverOpt, base64).tupled.mapValidated(PathOrJson.parse)
  val configOpt = Opts.option[String]("config", "Snowflake Loader JSON config, FS path or base64-encoded")
  val config = (configOpt, base64).tupled.mapValidated(PathOrJson.parse)

  val setupOpt = (config, steps, dryRun, resolver).mapN { (config, steps, dry, r) => Loader.SetupRaw(config, r, steps, dry) }
  val setup = Opts.subcommand(Command("setup", "Perform setup actions", true)(setupOpt))

  val migrateOpt = (config, dryRun, resolver, version).mapN { (config, dry, r, v) => Loader.MigrateRaw(config, r, v, dry) }
  val migrate = Opts.subcommand(Command("migrate", "Load data into a warehouse", true)(migrateOpt))

  val loadOpt = (config, dryRun, resolver).mapN { (config, dry, r) => Loader.LoadRaw(config, r, dry) }
  val load = Opts.subcommand(Command("load", "Load data into a warehouse", true)(loadOpt))

  val loaderCommand = Command("snowplow-snowflake-loader", "Snowplow Database orchestrator")(load.orElse(setup).orElse(migrate))

  def parse(args: Seq[String]) = {
    loaderCommand.parse(args)
      .leftMap(_.toString)
      .flatMap { raw => Loader.init(raw) }
  }

}

