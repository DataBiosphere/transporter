package org.broadinstitute.transporter.transfer

import java.time.Instant
import java.util.UUID

import cats.effect.{Clock, ContextShift, IO, Resource, Timer}
import cats.implicits._
import doobie._
import doobie.implicits._
import fs2.kafka.CommittableOffsetBatch
import fs2.{Chunk, Stream}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.{Constants, DoobieInstances}
import org.broadinstitute.transporter.kafka.KafkaConsumer
import org.broadinstitute.transporter.kafka.config.KafkaConfig

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
  progressConsumer: KafkaConsumer[Json],
  resultConsumer: KafkaConsumer[(TransferResult, Json)]
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
    * incremental progress messages collected from Kafka.
    */
  private[transfer] def markTransfersInProgress(
    progress: Chunk[TransferMessage[Json]]
  ): IO[Int] = {
    val statuses = List(TransferStatus.Submitted, TransferStatus.InProgress)
      .map(_.entryName.toLowerCase)
      .mkString("('", "','", "')")
    for {
      now <- getNow
      statusUpdates = progress.map { message =>
        (
          TransferStatus.InProgress: TransferStatus,
          message.message,
          message.ids.transfer,
          message.ids.request
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

  /**
    * Update Transporter's view of a set of transfers based on
    * terminal results collected from Kafka.
    */
  private[transfer] def recordTransferResults(
    results: Chunk[TransferMessage[(TransferResult, Json)]]
  ): IO[Int] =
    for {
      now <- getNow
      statusUpdates = results.map {
        case TransferMessage(ids, (result, info)) =>
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
    kafkaConfig: KafkaConfig
  )(implicit cs: ContextShift[IO], t: Timer[IO]): Resource[IO, TransferListener] =
    for {
      progressConsumer <- KafkaConsumer.ofTopic[Json](
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
      new TransferListener(dbClient, progressConsumer, resultConsumer)
    }
}
