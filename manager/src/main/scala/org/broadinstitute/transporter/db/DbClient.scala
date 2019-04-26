package org.broadinstitute.transporter.db

import java.util.UUID

import cats.effect.{Clock, ContextShift, IO, Resource}
import cats.implicits._
import doobie.hi._
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.postgres.{Instances => PostgresInstances}
import doobie.postgres.circe.Instances.JsonInstances
import doobie.util.{ExecutionContexts, Get, Put}
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.update.Update
import io.chrisdavenport.fuuid.FUUID
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.config.DbConfig
import org.broadinstitute.transporter.queue.{Queue, QueueSchema}
import org.broadinstitute.transporter.transfer._

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
    * @param id unique ID to assign to the new queue
    * @param queue information to persist about the new queue
    */
  def createQueue(id: FUUID, queue: Queue): IO[Unit]

  /** Update the expected JSON schema for the queue with the given ID. */
  def patchQueueSchema(id: FUUID, schema: QueueSchema): IO[Unit]

  /**
    * Remove the queue resource with the given ID from the DB.
    *
    * No-ops if no such queue exists in the DB.
    */
  def deleteQueue(id: FUUID): IO[Unit]

  /** Pull the queue resource with the given name from the DB, if it exists. */
  def lookupQueue(name: String): IO[Option[(FUUID, Queue)]]

  /**
    * Insert a top-level "transfer request" row and corresponding
    * per-request "transfer" rows into the DB, and return the generated
    * unique IDs.
    *
    * @param queueId ID of the queue to associate with the top-level transfer
    * @param request top-level description of the batch request to insert
    */
  def recordTransferRequest(
    queueId: FUUID,
    request: TransferRequest
  ): IO[(FUUID, List[(FUUID, Json)])]

  /** Fetch summary info for transfers registered under a queue/request pair. */
  def lookupTransfers(
    queueId: FUUID,
    requestId: FUUID
  ): IO[Map[TransferStatus, (Long, Vector[Json])]]

  /**
    * Delete the top-level description, and individual transfer descriptions,
    * of a batch transfer request.
    */
  def deleteTransferRequest(id: FUUID): IO[Unit]

  /**
    * Update the current view of the status of a set of in-flight transfers
    * based on a batch of results returned by some number of agents.
    *
    * @param results batch of ID -> result pairs pulled from Kafka which
    *                should be pushed into the DB
    */
  def updateTransfers(results: List[(FUUID, TransferSummary[Option[Json]])]): IO[Unit]
}

object DbClient extends PostgresInstances with JsonInstances {

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
    implicit cs: ContextShift[IO],
    clk: Clock[IO]
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
  implicit val fuuidGet: Get[FUUID] = Get[UUID].map(FUUID.fromUUID)
  implicit val fuuidPut: Put[FUUID] = Put[UUID].contramap(FUUID.Unsafe.toUUID)

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
    implicit cs: ContextShift[IO],
    clk: Clock[IO]
  ) extends DbClient {

    private val logger = Slf4jLogger.getLogger[IO]
    private implicit val logHandler: LogHandler = DbLogHandler(logger)

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

    override def createQueue(id: FUUID, queue: Queue): IO[Unit] =
      sql"""insert into queues
            (id, name, request_topic, progress_topic, response_topic, request_schema)
            values (
              $id,
              ${queue.name},
              ${queue.requestTopic},
              ${queue.progressTopic},
              ${queue.responseTopic},
              ${queue.schema}
            )""".update.run.transact(transactor).flatMap { numUpdated =>
        if (numUpdated == 1) {
          IO.unit
        } else {
          IO.raiseError(
            new RuntimeException(s"Queue INSERT command for $id updated $numUpdated rows")
          )
        }
      }

    override def patchQueueSchema(id: FUUID, schema: QueueSchema): IO[Unit] =
      sql"""update queues set request_schema = $schema where id = $id""".update.run.void
        .transact(transactor)

    override def deleteQueue(id: FUUID): IO[Unit] =
      sql"""delete from queues where id = $id""".update.run.void.transact(transactor)

    override def lookupQueue(name: String): IO[Option[(FUUID, Queue)]] =
      sql"""select id, name, request_topic, progress_topic, response_topic, request_schema
          from queues where name = $name"""
        .query[(FUUID, Queue)]
        .option
        .transact(transactor)

    override def recordTransferRequest(
      queueId: FUUID,
      request: TransferRequest
    ): IO[(FUUID, List[(FUUID, Json)])] =
      for {
        requestId <- FUUID.randomFUUID[IO]
        transfersById <- request.transfers.traverse { transfer =>
          FUUID.randomFUUID[IO].map(_ -> transfer)
        }
        now <- clk.realTime(scala.concurrent.duration.MILLISECONDS)
        _ <- insertRequest(queueId, requestId, transfersById, now).transact(transactor)
      } yield {
        (requestId, transfersById)
      }

    override def lookupTransfers(
      queueId: FUUID,
      requestId: FUUID
    ): IO[Map[TransferStatus, (Long, Vector[Json])]] =
      // 'coalesce' needed to strip the nulls out of the results.
      sql"""select t.status, count(*), coalesce(json_agg(t.info) filter (where t.info is not null), '[]')
            from transfers t
            left join transfer_requests r on t.request_id = r.id
            left join queues q on r.queue_id = q.id
            where q.id = $queueId and r.id = $requestId
            group by t.status"""
        .query[(TransferStatus, Long, Json)]
        .to[List]
        .map(
          // Ideally we could pull `Vector[Json]` from the DB directly, but doobie
          // can't handle mapping PG arrays of complex types.
          _.map { case (s, i, j) => (s, (i, j.asArray.getOrElse(Vector.empty))) }.toMap
        )
        .transact(transactor)

    override def deleteTransferRequest(id: FUUID): IO[Unit] =
      sql"""delete from transfer_requests where id = $id""".update.run.void
        .transact(transactor)

    override def updateTransfers(
      summary: List[(FUUID, TransferSummary[Option[Json]])]
    ): IO[Unit] = {
      val newStatuses = summary.map {
        case (id, s) =>
          val status = s.result match {
            case TransferResult.Success      => TransferStatus.Succeeded
            case TransferResult.FatalFailure => TransferStatus.Failed
          }
          (status, s.info, id)
      }

      Update[(TransferStatus, Option[Json], FUUID)](
        "update transfers set status = ?, info = ? where id = ?"
      ).updateMany(newStatuses)
        .void
        .transact(transactor)
    }

    /**
      * Build a transaction which will insert all of the rows (top-level and individual)
      * associated with a batch of transfer requests.
      */
    private def insertRequest(
      queueId: FUUID,
      requestId: FUUID,
      transfersById: List[(FUUID, Json)],
      nowMillis: Long
    ): ConnectionIO[Unit] = {
      val insertRequest =
        sql"""insert into transfer_requests (id, queue_id) values ($requestId, $queueId)""".update.run.void

      val insertTransfers = Update[(FUUID, Json)](
        s"""insert into transfers (id, request_id, status, body, info, submitted_at)
            values (?, '$requestId', '${TransferStatus.Submitted.entryName}', ?, NULL, TO_TIMESTAMP($nowMillis::double precision / 1000))"""
      ).updateMany(transfersById).void

      insertRequest.flatMap(_ => insertTransfers)
    }
  }
}
