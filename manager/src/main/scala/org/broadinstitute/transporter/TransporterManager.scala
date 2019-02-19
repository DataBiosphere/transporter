package org.broadinstitute.transporter

import cats.effect.{ExitCode, IO, IOApp, Resource}
import doobie.util.ExecutionContexts
import org.broadinstitute.transporter.api.ManagerApi
import org.http4s.server.blaze.BlazeServerBuilder

import scala.concurrent.ExecutionContext

object TransporterManager extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    blockingEc.use(bindAndRun)

  private def blockingEc: Resource[IO, ExecutionContext] =
    ExecutionContexts.cachedThreadPool[IO]

  private def bindAndRun(blockingEc: ExecutionContext): IO[ExitCode] = {
    val routes = new ManagerApi(
      "0.0.0-SNAP",
      //"3.20.8",
      blockingEc
    ).routes

    BlazeServerBuilder[IO]
      .bindHttp(port = 8080, host = "0.0.0.0")
      .withHttpApp(routes)
      .serve
      .compile
      .lastOrError
  }
}
