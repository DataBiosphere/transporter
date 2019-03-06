package org.broadinstitute.transporter

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Decoder
import org.broadinstitute.transporter.kafka.{KStreamsConfig, TransferStream}
import org.broadinstitute.transporter.queue.{Queue, QueueConfig}
import org.broadinstitute.transporter.transfer.TransferRunner
import org.http4s.Request
import org.http4s.circe.CirceEntityDecoder
import org.http4s.client.blaze.BlazeClientBuilder
import pureconfig.module.catseffect._

import scala.concurrent.ExecutionContext

abstract class TransporterAgent[Req](implicit val decoder: Decoder[Req])
    extends IOApp
    with CirceEntityDecoder {

  private val logger = Slf4jLogger.getLogger[IO]

  def runnerResource: Resource[IO, TransferRunner[Req]]

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

  private def lookupQueue(config: QueueConfig): IO[Option[Queue]] =
    BlazeClientBuilder[IO](ExecutionContext.global).resource.use { client =>
      val request = Request[IO](uri = config.managerUri / "queues" / config.queueName)
      logger
        .info(
          s"Getting Kafka topics for queue '${config.queueName}' from Transporter at ${config.managerUri}"
        )
        .flatMap(_ => client.expectOption[Queue](request))
    }

  private def runStream(config: KStreamsConfig, queue: Queue): IO[ExitCode] =
    runnerResource.use { runner =>
      for {
        stream <- TransferStream.build(config, queue, runner)
        _ <- IO.delay(stream.start())
        _ <- IO.cancelable[Unit](_ => IO.delay(stream.close()))
      } yield {
        ExitCode.Success
      }
    }
}
