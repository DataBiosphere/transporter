package org.broadinstitute.transporter.transfer

import java.time.Instant
import java.util.UUID

import cats.effect.{Async, Clock, ContextShift, IO, Resource, Timer}
import cats.implicits._
import doobie._
import doobie.implicits._
import fs2.kafka.CommittableOffsetBatch
import fs2.{Chunk, Stream}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.transporter.db.{Constants, DoobieInstances}
import org.broadinstitute.transporter.kafka.KafkaConsumer
import org.broadinstitute.transporter.kafka.config.KafkaConfig
import org.broadinstitute.transporter.transfer.api.TransferRequest

/**
  * Component responsible for pulling transfer updates from Kafka.
  *
  * @param dbClient client which can interact with Transporter's backing DB
  * @param progressConsumer client which can pull incremental progress updates
  *                         for transfers from Kafka
  * @param resultConsumer client which can pull transfer result messages from
  *                       Kafka
  */
class TransferListener private[transfer] (
  dbClient: Transactor[IO],
  progressConsumer: KafkaConsumer[(Int, Json)],
  resultConsumer: KafkaConsumer[(TransferResult, Json)],
  controller: TransferController
)(implicit cs: ContextShift[IO], clk: Clock[IO]) {
  import Constants._
  import DoobieInstances._

  private val logger = Slf4jLogger.getLogger[IO]

  /**
    * Stream which, when run, pulls transfer updates from Kafka and records
    * them in the manager's DB.
    *
    * The stream emits the number of messages pulled from Kafka after each
    * batch is persisted to the DB.
    */
  def listen: Stream[IO, Int] = {
    val progressStream = progressConsumer.stream.evalMap { chunk =>
      markTransfersInProgress(chunk.map { case (update, _) => update })
        .map(_ -> CommittableOffsetBatch.fromFoldable(chunk.map {
          case (_, offset) => offset
        }))
    }.evalTap {
      case (n, _) => logger.info(s"Recorded $n progress updates")
    }
    val resultStream = resultConsumer.stream.evalMap { chunk =>
      recordTransferResults(chunk.map { case (result, _) => result })
        .map(_ -> CommittableOffsetBatch.fromFoldable(chunk.map {
          case (_, offset) => offset
        }))
    }.evalTap {
      case (n, _) => logger.info(s"Recorded $n transfer results")
    }

    progressStream.mergeHaltBoth(resultStream).evalMap {
      case (n, offsetBatch) => offsetBatch.commit.as(n)
    }
  }

  private def getNow: IO[Instant] =
    clk.realTime(scala.concurrent.duration.MILLISECONDS).map(Instant.ofEpochMilli)

  /**
    * Update Transporter's view of a set of transfers based on
    * incremental progress messages collected from Kafka.
    */
  private[transfer] def markTransfersInProgress(
    progress: Chunk[TransferMessage[(Int, Json)]]
  ): IO[Int] = {
    val statusUpdates = progress.toVector
      .groupBy(_.ids)
      .mapValues(_.maxBy(_.message._1))
      .values
      .toVector
      .sortBy(_.ids.transfer)
      .map {
        case TransferMessage(ids, (stepCount, message)) =>
          (message, stepCount, ids.transfer, ids.request, stepCount)
      }

    val statuses = List(TransferStatus.Submitted, TransferStatus.InProgress)
      .map(_.entryName.toLowerCase)
      .mkString("('", "','", "')")

    for {
      now <- getNow
      numUpdated <- Update[(Json, Int, UUID, UUID, Int)](
        s"""UPDATE $TransfersTable
           |SET status = '${TransferStatus.InProgress.entryName}',
           |updated_at = ${timestampSql(now)}, info = ?, steps_run = ?
           |FROM $RequestsTable
           |WHERE $TransfersTable.request_id = $RequestsTable.id
           |AND $TransfersTable.status IN $statuses
           |AND $TransfersTable.id = ?
           |AND $RequestsTable.id = ?
           |AND steps_run < ?""".stripMargin
      ).updateMany(statusUpdates).transact(dbClient)
    } yield {
      numUpdated
    }
  }

  /** Expand transfers that need to be expanded and update the processed transfers. */
  private[transfer] def processUpdates(
    statusUpdates: Vector[(TransferStatus, Json, UUID, UUID)],
    now: Instant
  ): IO[Int] = {
    val transaction = for {
      finalStatusUpdates <- statusUpdates.traverse {
        case (TransferStatus.Expanded, info, transferId, requestId) =>
          for {
            // get the priorities associated with the expanded transfers
            priority <- List(
              Fragment.const(s"SELECT priority FROM $TransfersTable"),
              fr"WHERE id = $transferId"
            ).combineAll.query[Short].unique
            transfers <- info.as[List[Json]].liftTo[ConnectionIO]
            transferRequests = transfers.map(TransferRequest(_, None))
            // create the new transfers under the original transfer requestId and priority
            transferIds <- controller.recordTransferRequest(
              transferRequests,
              requestId,
              priority
            )
          } yield {
            (
              TransferStatus.Expanded: TransferStatus,
              ExpandedTransferIds(transferIds).asJson,
              transferId,
              requestId
            )
          }
        case other =>
          Async[ConnectionIO].pure(other)
      }

      numUpdated <- Update[(TransferStatus, Json, UUID, UUID)](
        s"""UPDATE $TransfersTable
           |SET status = ?, info = ?, updated_at = ${timestampSql(now)}
           |FROM $RequestsTable
           |WHERE $TransfersTable.request_id = $RequestsTable.id
           |AND $TransfersTable.id = ?
           |AND $RequestsTable.id = ?""".stripMargin
      ).updateMany(finalStatusUpdates)

    } yield {
      numUpdated
    }
    transaction.transact(dbClient)
  }

  /**
    * Update Transporter's view of a set of transfers based on
    * terminal results collected from Kafka.
    */
  private[transfer] def recordTransferResults(
    results: Chunk[TransferMessage[(TransferResult, Json)]]
  ): IO[Int] =
    for {
      now <- getNow
      statusUpdates = results.toVector
        .sortBy(_.ids.transfer)
        .map {
          case TransferMessage(ids, (result, info)) =>
            val status = result match {
              case TransferResult.Success      => TransferStatus.Succeeded
              case TransferResult.FatalFailure => TransferStatus.Failed
              case TransferResult.Expanded     => TransferStatus.Expanded
            }
            (status, info, ids.transfer, ids.request)
        }

      numUpdated <- processUpdates(statusUpdates, now)
    } yield {
      numUpdated
    }
}

object TransferListener {
  import org.broadinstitute.transporter.kafka.Serdes._

  /**
    * Build a listener wrapped in setup / teardown logic for its underlying clients.
    *
    * @param dbClient client which can interact with Transporter's backing DB
    * @param kafkaConfig parameters determining how Transporter should communicate
    *                    with Kafka
    */
  def resource(
    dbClient: Transactor[IO],
    kafkaConfig: KafkaConfig,
    controller: TransferController
  )(implicit cs: ContextShift[IO], t: Timer[IO]): Resource[IO, TransferListener] =
    for {
      progressConsumer <- KafkaConsumer.ofTopic[(Int, Json)](
        kafkaConfig.topics.progressTopic,
        kafkaConfig.connection,
        kafkaConfig.consumer
      )
      resultConsumer <- KafkaConsumer.ofTopic[(TransferResult, Json)](
        kafkaConfig.topics.resultTopic,
        kafkaConfig.connection,
        kafkaConfig.consumer
      )
    } yield {
      new TransferListener(dbClient, progressConsumer, resultConsumer, controller)
    }
}
