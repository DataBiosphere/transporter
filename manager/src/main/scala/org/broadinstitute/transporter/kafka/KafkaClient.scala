package org.broadinstitute.transporter.kafka

import java.util.concurrent.{CancellationException, CompletionException}

import cats.effect._
import fs2.kafka.AdminClientSettings
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.KafkaFuture

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class KafkaClient private[kafka] (
  adminClient: AdminClient,
  blockingEc: ExecutionContext
)(implicit cs: ContextShift[IO], t: Timer[IO]) {

  import KafkaClient.KafkaFutureSyntax

  def checkReady: IO[Boolean] = {
    val clusterId = IO.suspend(adminClient.describeCluster().clusterId().cancelable)

    val okCheck = for {
      id <- cs.evalOn(blockingEc)(clusterId)
      _ <- IO.delay(println(s"Got $id"))
    } yield {
      true
    }

    okCheck.timeoutTo(5.seconds, IO.delay {
      println("TIMING OUT!!!!!")
      false
    })
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
    implicit cs: ContextShift[IO],
    t: Timer[IO]
  ): Resource[IO, KafkaClient] = {
    val settings = AdminClientSettings.Default
      .withBootstrapServers(config.bootstrapServers.mkString(","))
      .withClientId(config.clientId)
      .withCloseTimeout(1.second)

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
