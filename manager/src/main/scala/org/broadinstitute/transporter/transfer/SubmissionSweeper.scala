package org.broadinstitute.transporter.transfer

import cats.effect.{ContextShift, IO, Resource, Timer}
import fs2.Stream
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.{KafkaProducer, Serdes}
import org.broadinstitute.transporter.kafka.config.KafkaConfig

import scala.concurrent.duration.FiniteDuration

class SubmissionSweeper private[transfer] (
  producer: KafkaProducer[TransferRequest[Json]],
  dbClient: DbClient
)(implicit t: Timer[IO]) {

  private val logger = Slf4jLogger.getLogger[IO]

  def runSweeper(sweepFrequency: FiniteDuration): IO[Unit] = {

    Stream.fixedDelay[IO](sweepFrequency).evalMap(_ => sweepSubmissions).compile.drain
  }

  private[transfer] def sweepSubmissions: IO[Unit] =
    logger.info("Sweeping for eligible transfer submissions...").flatMap { _ =>
      dbClient.submitTransfers { submissions =>
        for {
          _ <- logger.info("Submitting eligible transfers to Kafka...")
          _ <- producer.submit(submissions)
          _ <- logger.info("Submission sweep completed")
        } yield ()
      }
    }
}

object SubmissionSweeper {

  def resource(dbClient: DbClient, kafkaConfig: KafkaConfig)(
    implicit cs: ContextShift[IO],
    t: Timer[IO]
  ): Resource[IO, SubmissionSweeper] =
    for {
      producer <- KafkaProducer.resource(
        kafkaConfig,
        Serdes.encodingSerializer[TransferRequest[Json]]
      )
    } yield {
      new SubmissionSweeper(producer, dbClient)
    }
}
