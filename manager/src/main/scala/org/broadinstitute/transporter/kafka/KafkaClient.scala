package org.broadinstitute.transporter.kafka

import java.util.concurrent.{CancellationException, CompletionException}

import cats.effect._
import cats.implicits._
import fs2.kafka.AdminClientSettings
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.KafkaFuture

import scala.concurrent.{ExecutionContext, TimeoutException}

class KafkaClient private[kafka] (
  adminClient: AdminClient,
  blockingEc: ExecutionContext
)(implicit cs: ContextShift[IO]) {

  import KafkaClient.KafkaFutureSyntax

  private val logger = Slf4jLogger.getLogger[IO]

  def checkReady: IO[Boolean] = {
    val clusterId = IO.suspend(adminClient.describeCluster().clusterId().cancelable)

    val okCheck = for {
      _ <- logger.info("Running status check against Kafka cluster...")
      id <- cs.evalOn(blockingEc)(clusterId)
      _ <- logger.debug(s"Got cluster ID $id")
    } yield {
      true
    }

    okCheck.handleErrorWith {
      case _: TimeoutException =>
        logger.error("Kafka status check timed out").as(false)
      case err =>
        logger.error(err)("Kafka status check hit error").as(false)
    }
  }
}

object KafkaClient {

  implicit final class KafkaFutureSyntax[A](
    private val future: KafkaFuture[A]
  ) extends AnyVal {

    def cancelToken: CancelToken[IO] =
      IO.delay { future.cancel(true); () }

    def cancelable: IO[A] =
      IO.cancelable { cb =>
        future.whenComplete { (a: A, t: Throwable) =>
          t match {
            case null                                         => cb(Right(a))
            case _: CancellationException                     => ()
            case e: CompletionException if e.getCause != null => cb(Left(e.getCause))
            case e                                            => cb(Left(e))
          }
        }.cancelToken
      }
  }

  def resource(config: KafkaConfig, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, KafkaClient] = {
    val settings = AdminClientSettings.Default
      .withBootstrapServers(config.bootstrapServers.mkString(","))
      .withClientId(config.clientId)
      .withRequestTimeout(config.timeouts.requestTimeout)
      .withCloseTimeout(config.timeouts.closeTimeout)

    val initClient =
      cs.evalOn(blockingEc)(settings.adminClientFactory.create[IO](settings))

    val closeClient = (client: AdminClient) =>
      cs.evalOn(blockingEc)(IO.delay {
        client.close(settings.closeTimeout.length, settings.closeTimeout.unit)
      })

    Resource
      .make(initClient)(closeClient)
      .map(new KafkaClient(_, blockingEc))
  }
}
