package org.broadinstitute.transporter.db

import java.util.UUID

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.postgres.circe.Instances
import doobie.util.{ExecutionContexts, Get, Put}
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import io.chrisdavenport.fuuid.FUUID
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.queue.{Queue, QueueRequest, QueueSchema}

import scala.concurrent.ExecutionContext

/**
  * Client responsible for sending requests to / parsing responses from
  * Transporter's backing DB.
  */
trait DbClient {

  /** Check if the client can interact with the backing DB. */
  def checkReady: IO[Boolean]

  /**
    * Create a new queue resource in the DB.
    *
    * Will fail if a queue already exists with the input's name.
    *
    * @param request user-specified information defining pieces of
    *                the queue to create
    */
  def createQueue(request: QueueRequest): IO[Queue]

  /**
    * Remove the queue resource with the given name from the DB.
    *
    * No-ops if no such queue exists in the DB.
    */
  def deleteQueue(name: String): IO[Unit]

  /** Pull the queue resource with the given name from the DB, if it exists. */
  def lookupQueueInfo(name: String): IO[Option[DbClient.QueueInfo]]
}

object DbClient {

  type QueueInfo = (UUID, String, String, QueueSchema)

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
      connectionContext <- ExecutionContexts.fixedThreadPool[IO](MaxDbConnections)
      // NOTE: Lines beneath here are from doobie's implementation of `HikariTransactor.newHikariTransactor`.
      // Have to open up the guts to set detailed configuration.
      _ <- Resource.liftF(IO.delay(Class.forName(config.driverClassname)))
      transactor <- HikariTransactor.initial[IO](
        connectEC = connectionContext,
        transactEC = blockingEc
      )
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
      new Impl(transactor)
    }

  /**
    * Concrete implementation of our DB client used by mainline code.
    *
    * @param transactor wrapper around a source of DB connections which can
    *                   actually run SQL
    * @param cs proof of the ability to shift IO-wrapped computations
    *           onto other threads
    * @see https://tpolecat.github.io/doobie/docs/01-Introduction.html for
    *      documentation on `doobie`, the library used for DB access
    */
  private[db] class Impl(transactor: Transactor[IO])(
    implicit cs: ContextShift[IO]
  ) extends DbClient
      with Instances.JsonInstances {

    private val logger = Slf4jLogger.getLogger[IO]
    private implicit val logHandler: LogHandler = DbLogHandler(logger)

    /*
     * "Orphan" instances describing how to get/put our custom JSON schema
     * type from/into the DB.
     *
     * We define them here instead of in the schema class' companion object
     * because the schema class lives in our "common" subproject, which doesn't
     * need to depend on anything doobie-related.
     */
    implicit val schemaGet: Get[QueueSchema] = pgDecoderGet
    implicit val schemaPut: Put[QueueSchema] = pgEncoderPut

    // Glue for the "safe" UUID type our lib generates.
    implicit val fuuidPut: Put[FUUID] = Put[UUID].contramap(FUUID.Unsafe.toUUID)

    /** Check if the client can interact with the backing DB. */
    override def checkReady: IO[Boolean] = {
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

    /**
      * Create a new queue resource in the DB.
      *
      * Will fail if a queue already exists with the input's name.
      *
      * @param request user-specified information defining pieces of
      *                the queue to create
      */
    override def createQueue(request: QueueRequest): IO[Queue] =
      for {
        // Build a unique ID for the new resource.
        _ <- logger.debug(s"Generating UUID for ${request.name}")
        uuid <- FUUID.randomFUUID[IO]

        // Use the ID to fill in Kafka information
        requestTopic = s"transporter.requests.$uuid"
        responseTopic = s"transporter.responses.$uuid"
        insertStatement = sql"""insert into queues (id, name, request_topic, response_topic, request_schema)
            values ($uuid, ${request.name}, $requestTopic, $responseTopic, ${request.schema})"""

        // Run the insert statement, returning fields which should be passed back to the user.
        queue <- insertStatement.update
          .withUniqueGeneratedKeys[Queue](
            "name",
            "request_topic",
            "response_topic",
            "request_schema"
          )
          .transact(transactor)
      } yield {
        queue
      }

    /**
      * Remove the queue resource with the given name from the DB.
      *
      * No-ops if no such queue exists in the DB.
      */
    override def deleteQueue(name: String): IO[Unit] =
      sql"""delete from queues where name = $name""".update.run.void.transact(transactor)

    /** Pull the queue resource with the given name from the DB, if it exists. */
    override def lookupQueueInfo(name: String): IO[Option[QueueInfo]] =
      sql"""select id, request_topic, response_topic, request_schema
          from queues where name = $name"""
        .query[(UUID, String, String, QueueSchema)]
        .option
        .transact(transactor)
  }
}
