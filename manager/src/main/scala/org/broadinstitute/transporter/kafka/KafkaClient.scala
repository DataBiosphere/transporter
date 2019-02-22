package org.broadinstitute.transporter.kafka

import java.util.concurrent.{CancellationException, CompletionException}

import cats.effect._
import cats.effect.concurrent.Deferred
import fs2.kafka.AdminClientSettings
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.KafkaFuture

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class KafkaClient private[kafka] (
  clientPromise: Deferred[IO, AdminClient],
  blockingEc: ExecutionContext
)(implicit cs: ContextShift[IO], t: Timer[IO]) {

  import KafkaClient.KafkaFutureSyntax

  def checkReady: IO[Boolean] = {
    val okCheck = for {
      adminClient <- clientPromise.get
      clusterId = IO.suspend(adminClient.describeCluster().clusterId().cancelable)
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

    Resource
      .make(initClient(settings, blockingEc)) {
        case (fiber, promise) => closeClient(fiber, promise, settings.closeTimeout)
      }
      .map {
        case (_, promise) => new KafkaClient(promise, blockingEc)
      }
  }

  private def initClient(settings: AdminClientSettings, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO]
  ): IO[(Fiber[IO, Unit], Deferred[IO, AdminClient])] =
    Deferred[IO, AdminClient].flatMap { promise =>
      val createClient = for {
        client <- cs.evalOn(blockingEc)(settings.adminClientFactory.create[IO](settings))
        _ <- promise.complete(client)
      } yield {
        ()
      }

      createClient.start.map(_ -> promise)
    }

  private def closeClient(
    initFiber: Fiber[IO, Unit],
    clientPromise: Deferred[IO, AdminClient],
    timeout: FiniteDuration
  )(implicit cs: ContextShift[IO], t: Timer[IO]): IO[Unit] =
    initFiber.cancel.flatMap { _ =>
      val tryCancel = clientPromise.get.flatMap(
        admin => IO.delay(admin.close(timeout.length, timeout.unit))
      )
      tryCancel.timeoutTo(timeout, IO.unit)
    }
}
