package org.broadinstitute.transporter

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.streams.KafkaStreams
import org.broadinstitute.transporter.kafka.{KStreamsConfig, TransferStream}
import org.broadinstitute.transporter.queue.{Queue, QueueConfig}
import org.broadinstitute.transporter.transfer.TransferRunner
import org.http4s.Request
import org.http4s.circe.CirceEntityDecoder
import org.http4s.client.blaze.BlazeClientBuilder
import pureconfig.module.catseffect._

import scala.concurrent.ExecutionContext

/**
  * Main entry-point for Transporter agent programs.
  *
  * Extending this base class should provide concrete agent programs with
  * the full framework needed to hook into Transporter's Kafka infrastructure
  * and run data transfers.
  */
abstract class TransporterAgent extends IOApp with CirceEntityDecoder {

  private val logger = Slf4jLogger.getLogger[IO]

  /**
    * Construct the agent component which can actually run data transfers.
    *
    * Modeled as a `Resource` so agent programs can hook in setup / teardown
    * logic for config, thread pools, etc.
    */
  def runnerResource: Resource[IO, TransferRunner]

  /** [[IOApp]] equivalent of `main`. */
  final override def run(args: List[String]): IO[ExitCode] =
    loadConfigF[IO, AgentConfig]("org.broadinstitute.transporter").flatMap { config =>
      for {
        maybeQueue <- lookupQueue(config.queue)
        retCode <- maybeQueue match {
          case Some(queue) => runStream(config.kafka, queue)
          case None =>
            logger.error(s"No such queue: ${config.queue.queueName}").as(ExitCode.Error)
        }
      } yield {
        retCode
      }
    }

  /** Query the configured Transporter manager for information about a queue. */
  private def lookupQueue(config: QueueConfig): IO[Option[Queue]] =
    BlazeClientBuilder[IO](ExecutionContext.global).resource.use { client =>
      val request = Request[IO](uri = config.managerUri / "queues" / config.queueName)
      logger
        .info(
          s"Getting Kafka topics for queue '${config.queueName}' from Transporter at ${config.managerUri}"
        )
        .flatMap(_ => client.expectOption[Queue](request))
    }

  /**
    * Build and launch the Kafka stream which receives, processes, and reports
    * on data transfer requests.
    *
    * This method should only return if the underlying stream is interrupted
    * (i.e. by a Ctrl-C).
    */
  private def runStream(config: KStreamsConfig, queue: Queue): IO[ExitCode] = {
    runnerResource.use { runner =>
      val topology = TransferStream.build(queue, runner)

      for {
        stream <- IO.delay(new KafkaStreams(topology, config.asProperties))
        _ <- IO.delay(stream.start())
        _ <- IO.cancelable[Unit](_ => IO.delay(stream.close()))
      } yield {
        ExitCode.Success
      }
    }
  }
}
