package org.broadinstitute.transporter.db

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.queue.Queue

import scala.concurrent.ExecutionContext

/**
  * Client responsible for sending requests to / parsing responses from
  * Transporter's backing DB.
  *
  * @param transactor wrapper around a source of DB connections which can
  *                   actually run SQL
  * @param cs proof of the ability to shift IO-wrapped computations
  *           onto other threads
  * @see https://tpolecat.github.io/doobie/docs/01-Introduction.html for
  *      documentation on `doobie`, the library used for DB access
  */
class DbClient private[db] (transactor: Transactor[IO])(
  implicit cs: ContextShift[IO]
) {

  private val logger = Slf4jLogger.getLogger[IO]

  /** Check if the client can interact with the backing DB. */
  def checkReady: IO[Boolean] = {
    val check = for {
      _ <- logger.info("Running status check against DB...")
      isValid <- doobie.free.connection.isValid(0).transact(transactor)
    } yield {
      isValid
    }

    check.handleErrorWith { err =>
      logger.error(err)("DB status check hit error").as(false)
    }
  }

  def insertQueue(queue: Queue): IO[Unit] = {
    val insert =
      sql"""insert into queues
            (name, request_topic, response_topic, request_schema)
            values
            (${queue.name}, ${queue.requestTopic}, ${queue.responseTopic}, ${queue.schema})"""

    insert.update.run.void.transact(transactor)
  }

  def lookupQueue(name: String): IO[Option[Queue]] =
    sql"""select name, request_topic, response_topic, request_schema
          from queues where name = '$name'"""
      .query[Queue]
      .option
      .transact(transactor)
}

object DbClient {
  // Recommendation from Hikari docs:
  // https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing#the-formula
  private val MaxDbConnections = (2 * Runtime.getRuntime.availableProcessors) + 1

  /**
    * Construct a DB client, wrapped in logic which will:
    *   1. Automatically spin up a Hikari connection pool on startup, and
    *   2. Automatically clean up the connection pool on shutdown
    *
    * @param config settings for the underlying DB transactor powering the client
    * @param blockingEc execution context which should run all blocking
    *                   I/O required by the DB transactor
    * @param cs proof of the ability to shift IO-wrapped computations
    *           onto other threads
    */
  def resource(config: DbConfig, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, DbClient] =
    for {
      // Recommended by doobie docs: use a fixed-size thread pool to avoid flooding the DB.
      transactionContext <- ExecutionContexts.fixedThreadPool[IO](MaxDbConnections)
      // NOTE: Lines beneath here are from doobie's implementation of `HikariTransactor.newHikariTransactor`.
      // Have to open up the guts to set detailed configuration.
      _ <- Resource.liftF(IO.delay(Class.forName(config.driverClassname)))
      transactor <- HikariTransactor.initial[IO](blockingEc, transactionContext)
      _ <- Resource.liftF {
        transactor.configure { dataSource =>
          IO.delay {
            // Basic connection config:
            dataSource.setJdbcUrl(config.connectURL)
            dataSource.setUsername(config.username)
            dataSource.setPassword(config.password)

            // Turn knobs here:
            dataSource.setMaximumPoolSize(MaxDbConnections)
            dataSource.setConnectionTimeout(config.timeouts.connectionTimeout.toMillis)
            dataSource.setMaxLifetime(config.timeouts.maxConnectionLifetime.toMillis)
            dataSource.setValidationTimeout(
              config.timeouts.connectionValidationTimeout.toMillis
            )
            dataSource.setLeakDetectionThreshold(
              config.timeouts.leakDetectionThreshold.toMillis
            )
          }
        }
      }
    } yield {
      new DbClient(transactor)
    }
}
