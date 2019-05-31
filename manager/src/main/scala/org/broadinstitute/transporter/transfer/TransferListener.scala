package org.broadinstitute.transporter.transfer

import java.time.Instant
import java.util.UUID

import cats.effect.{Clock, ContextShift, IO}
import cats.implicits._
import doobie._
import doobie.implicits._
import fs2.kafka.CommittableOffsetBatch
import fs2.{Chunk, Stream}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.{Constants, DoobieInstances}
import org.broadinstitute.transporter.kafka.KafkaConsumer

class TransferListener(
  dbClient: Transactor[IO],
  progressConsumer: KafkaConsumer[Json],
  resultConsumer: KafkaConsumer[(TransferResult, Json)]
)(implicit cs: ContextShift[IO], clk: Clock[IO]) {
  import Constants._
  import DoobieInstances._

  private val logger = Slf4jLogger.getLogger[IO]

  def listen: Stream[IO, Int] = {
    val progressStream = progressConsumer.stream.evalMap { chunk =>
      markTransfersInProgress(chunk.map(_._1))
        .map(_ -> CommittableOffsetBatch.fromFoldable(chunk.map(_._2)))
    }.evalTap {
      case (n, _) => logger.info(s"Recorded $n progress updates")
    }
    val resultStream = resultConsumer.stream.evalMap { chunk =>
      recordTransferResults(chunk.map(_._1))
        .map(_ -> CommittableOffsetBatch.fromFoldable(chunk.map(_._2)))
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
    * terminal results collected from Kafka.
    */
  private[transfer] def recordTransferResults(
    results: Chunk[(TransferIds, (TransferResult, Json))]
  ): IO[Int] =
    for {
      now <- getNow
      statusUpdates = results.map {
        case (ids, (result, info)) =>
          val status = result match {
            case TransferResult.Success      => TransferStatus.Succeeded
            case TransferResult.FatalFailure => TransferStatus.Failed
          }
          (status, info, ids.transfer, ids.request)
      }
      numUpdated <- Update[(TransferStatus, Json, UUID, UUID)](
        s"""update $TransfersTable
           |set status = ?, info = ?, updated_at = ${timestampSql(now)}
           |from $RequestsTable
           |where $TransfersTable.request_id = $RequestsTable.id
           |and $TransfersTable.id = ? and $RequestsTable.id = ?""".stripMargin
      ).updateMany(statusUpdates).transact(dbClient)
    } yield {
      numUpdated
    }

  /**
    * Update Transporter's view of a set of transfers based on
    * incremental progress messages collected from Kafka.
    */
  private[transfer] def markTransfersInProgress(
    progress: Chunk[(TransferIds, Json)]
  ): IO[Int] = {
    val statuses = List(TransferStatus.Submitted, TransferStatus.InProgress)
      .map(_.entryName.toLowerCase)
      .mkString("('", "','", "')")
    for {
      now <- getNow
      statusUpdates = progress.map {
        case (ids, info) =>
          (
            TransferStatus.InProgress: TransferStatus,
            info,
            ids.transfer,
            ids.request
          )
      }
      numUpdated <- Update[(TransferStatus, Json, UUID, UUID)](
        s"""update $TransfersTable
           |set status = ?, info = ?, updated_at = ${timestampSql(now)}
           |from $RequestsTable
           |where $TransfersTable.request_id = $RequestsTable.id
           |and $TransfersTable.status in $statuses
           |and $TransfersTable.id = ? and $RequestsTable.id = ?""".stripMargin
      ).updateMany(statusUpdates).transact(dbClient)
    } yield {
      numUpdated
    }
  }
}
