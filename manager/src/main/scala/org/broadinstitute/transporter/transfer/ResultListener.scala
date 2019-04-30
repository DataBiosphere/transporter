package org.broadinstitute.transporter.transfer

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.config.KafkaConfig
import org.broadinstitute.transporter.kafka.{KafkaConsumer, Serdes}

/**
  * Component responsible for processing summary messages received
  * from Transporter agents over Kafka.
  *
  * Coordinates persisting results to the DB and resubmitting transfers
  * that fail on transient errors.
  */
class ResultListener private[transfer] (
  consumer: KafkaConsumer[TransferSummary[Json]],
  dbClient: DbClient
)(implicit cs: ContextShift[IO]) {

  private val logger = Slf4jLogger.getLogger[IO]

  /**
    * Launch the stream that will listen to & process all results
    * pulled from Kafka.
    *
    * NOTE: The returned `IO` won't complete except on error / cancellation;
    * run it concurrently as a background thread.
    */
  def processResults: IO[Unit] = consumer.runForeach(processBatch)

  /** Process a single batch of results received from some number of Transporter agents. */
  private[transfer] def processBatch(
    batch: List[KafkaConsumer.Attempt[TransferSummary[Json]]]
  ): IO[Unit] = {
    val (numMalformed, results) = batch.foldMap {
      case Right(res) => (0, List(res))
      case Left(_)    => (1, Nil)
    }

    val logPrefix = s"Recording ${results.length} transfer results"

    for {
      _ <- if (numMalformed > 0) {
        logger.warn(s"$logPrefix; ignoring $numMalformed malformed entities in batch")
      } else {
        logger.info(logPrefix)
      }
      _ <- dbClient.updateTransfers(results)
    } yield ()
  }
}

object ResultListener {

  def resource(dbClient: DbClient, kafkaConfig: KafkaConfig)(
    implicit cs: ContextShift[IO],
    t: Timer[IO]
  ): Resource[IO, ResultListener] =
    KafkaConsumer
      .resource(
        s"${KafkaConfig.ResponseTopicPrefix}.+".r,
        kafkaConfig,
        Serdes.decodingDeserializer[TransferSummary[Json]]
      )
      .map(new ResultListener(_, dbClient))
}
