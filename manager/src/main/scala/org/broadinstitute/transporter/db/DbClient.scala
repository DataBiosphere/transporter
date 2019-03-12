package org.broadinstitute.transporter.db

import java.util.UUID

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import doobie.hi._
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.postgres.circe.Instances
import doobie.util.{ExecutionContexts, Get, Put}
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.update.Update
import io.chrisdavenport.fuuid.FUUID
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.queue.{Queue, QueueRequest, QueueSchema}
import org.broadinstitute.transporter.transfer.TransferRequest

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

  def recordTransferRequest(
    queueId: UUID,
    request: TransferRequest
  ): IO[(UUID, List[(UUID, Json)])]

  def deleteTransferRequest(id: UUID): IO[Unit]
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

    override def createQueue(request: QueueRequest): IO[Queue] =
      for {
        uuid <- FUUID.randomFUUID[IO]
        requestTopic = s"transporter.requests.$uuid"
        responseTopic = s"transporter.responses.$uuid"

        insertStatement = sql"""insert into queues (id, name, request_topic, response_topic, request_schema)
            values ($uuid, ${request.name}, $requestTopic, $responseTopic, ${request.schema})"""
        _ <- insertStatement.update.run.transact(transactor)
      } yield {
        Queue(request.name, requestTopic, responseTopic, request.schema)
      }

    override def deleteQueue(name: String): IO[Unit] =
      sql"""delete from queues where name = $name""".update.run.void.transact(transactor)

    override def lookupQueueInfo(name: String): IO[Option[QueueInfo]] =
      sql"""select id, request_topic, response_topic, request_schema
          from queues where name = $name"""
        .query[(UUID, String, String, QueueSchema)]
        .option
        .transact(transactor)

    override def recordTransferRequest(
      queueId: UUID,
      request: TransferRequest
    ): IO[(UUID, List[(UUID, Json)])] =
      for {
        requestId <- FUUID.randomFUUID[IO]
        requestUuid = FUUID.Unsafe.toUUID(requestId)
        transfersById <- request.transfers.traverse { transfer =>
          FUUID.randomFUUID[IO].map(id => FUUID.Unsafe.toUUID(id) -> transfer)
        }
        _ <- insertRequest(queueId, requestUuid, transfersById.map(_._1))
          .transact(transactor)
      } yield {
        (requestUuid, transfersById)
      }

    private def insertRequest(
      queueId: UUID,
      requestId: UUID,
      transferIds: List[UUID]
    ): ConnectionIO[Unit] = {
      val insertRequest =
        sql"""insert into transfer_requests (id, queue_id, transfer_count)
              values($requestId, $queueId, ${transferIds.length})""".update.run.void

      val insertTransfers = Update[UUID](
        s"insert into transfers (id, request_id, status) values (?, '$requestId', 'submitted')"
      ).updateMany(transferIds).void

      for {
        _ <- insertRequest
        _ <- insertTransfers
      } yield {
        ()
      }
    }

    override def deleteTransferRequest(id: UUID): IO[Unit] =
      sql"""delete from transfer_requests where id = $id""".update.run.void
        .transact(transactor)
  }
}
