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
package connection

import cats.syntax.functor._
import cats.syntax.apply._
import cats.effect.{ Sync, IO }
import cats.effect.concurrent.Ref

import ast._
import com.snowplowanalytics.snowflake.loader.ast.Statement._
import com.snowplowanalytics.snowflake.core.Config

object DryRun {

  // TODO: it should be StateT instead of Ref
  // TODO: it should be parametrized, but Database.Connection does not have type parameters
  type Connection = Ref[IO, ConnectionState]

  /** Fake connection, only logging actions */
  final case class ConnectionState(messages: List[String], transaction: Option[String], transactionNum: Int) {
    def getMessages: List[String] =
      messages.reverse
  }

  object ConnectionState {
    val empty: ConnectionState = ConnectionState(Nil, None, 0)
  }

  def init: Database[IO] = new Stub

  /** Implementation that can be extended for unit tests */
  class Stub extends Database[IO] {
    def getConnection(config: Config): IO[Database.Connection] =
      for {
        logConnection <- Ref.of[IO, ConnectionState](ConnectionState(Nil, None, 0))
        _ <- log(logConnection, s"Connected to ${config.database} database")
      } yield Database.Connection.Dry(logConnection)

    def execute[S: Statement](connection: Database.Connection, ast: S): IO[Unit] =
      connection match {
        case Database.Connection.Dry(conn) => log(conn, ast)
        case Database.Connection.Jdbc(_) => invalidCall[IO]
      }

    def startTransaction(connection: Database.Connection, name: Option[String]): IO[Unit] =
      connection match {
        case Database.Connection.Dry(conn) => DryRun.startTransaction(conn, name)
        case Database.Connection.Jdbc(_) => invalidCall[IO]
      }

    def commitTransaction(connection: Database.Connection): IO[Unit] =
      connection match {
        case Database.Connection.Dry(conn) => DryRun.commitTransaction(conn)
        case Database.Connection.Jdbc(_) => invalidCall[IO]
      }

    def executeAndOutput[S: Statement](connection: Database.Connection, ast: S): IO[Unit] =
      connection match {
        case Database.Connection.Dry(conn) => log(conn, ast)
        case Database.Connection.Jdbc(_) => invalidCall[IO]
      }

    def rollbackTransaction(connection: Database.Connection): IO[Unit] =
      connection match {
        case Database.Connection.Dry(conn) => DryRun.rollbackTransaction(conn)
        case Database.Connection.Jdbc(_) => invalidCall[IO]
      }

    def executeAndCountRows[S: Statement](connection: Database.Connection, ast: S): IO[Int] =
      connection match {
        case Database.Connection.Dry(conn) => log(conn, ast).as(1)
        case Database.Connection.Jdbc(_) => invalidCall[IO].as(1)
      }

    def executeAndReturnResult[S: Statement](connection: Database.Connection, ast: S): IO[List[Map[String, Object]]] =
      connection match {
        case Database.Connection.Dry(conn) => log(conn, ast).as(List.empty)
        case Database.Connection.Jdbc(_) => invalidCall[IO].as(List.empty)
      }
  }

  private def log[S: Statement](state: Connection, statement: S) =
    state.update { s =>
      val updated = statement.getStatement.value :: s.messages
      s.copy(messages = updated)
    }

  private def log(state: Connection, message: String) =
    state.update { s =>
      val updated = message :: s.messages
      s.copy(messages = updated)
    }

  private def startTransaction(state: Connection, name: Option[String]) =
    for {
      connection <- state.get
      newName = name.getOrElse((connection.transactionNum + 1).toString)
      _ <- connection.transaction match {
        case Some(existing) =>
          log(state, s"Invalid state: new transaction ($newName) started until current [$existing] not committed")
        case None =>
          val updated = connection.copy(transactionNum = connection.transactionNum + 1, transaction = Some(newName))
          state.set(updated) *> log(state, s"New transaction ${name.getOrElse(" ")} started")
      }
    } yield ()

  private def commitTransaction(state: Connection) =
    state.get.flatMap { connection =>
      connection.transaction match {
        case Some(current) =>
          val updated = connection.copy(transaction = None)
          state.set(updated) *> log(state, s"Transaction [$current] successfully closed")
        case None =>
          log(state, "Invalid state: trying to close non-existent transaction")
      }
    }

  private def rollbackTransaction(state: Connection): IO[Unit] =
    state.get.flatMap { connection =>
      connection.transaction match {
        case Some(current) =>
          log(state, s"Transaction [$current] cancelled")
        case None =>
          log(state, "Invalid state: trying to rollback non-existent transaction")
      }
    }

  private def invalidCall[F[_]: Sync]: F[Unit] =
    Sync[F].raiseError(new IllegalStateException("DryRun Database was called with JDBC connection"))
}

