package org.broadinstitute.transporter

import cats.effect.{ExitCode, IO, IOApp, Resource}
import doobie.util.ExecutionContexts
import org.broadinstitute.transporter.api.ManagerApi
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.status.StatusController
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.Logger
import pureconfig.module.catseffect._

import scala.concurrent.ExecutionContext

object TransporterManager extends IOApp {

  override def run(args: List[String]): IO[ExitCode] =
    loadConfigF[IO, ManagerConfig]("org.broadinstitute.transporter")
      .flatMap(config => blockingEc.use(bindAndRun(config, _)))

  private def blockingEc: Resource[IO, ExecutionContext] =
    ExecutionContexts.cachedThreadPool[IO]

  private def bindAndRun(
    config: ManagerConfig,
    blockingEc: ExecutionContext
  ): IO[ExitCode] = {
    DbClient.resource(config.db, blockingEc).use { dbClient =>
      val app = ManagerApp(
        statusController = new StatusController(dbClient)
      )

      val routes = new ManagerApi(
        BuildInfo.version,
        BuildInfo.swaggerVersion,
        app,
        blockingEc
      ).routes

      BlazeServerBuilder[IO]
        .bindHttp(port = config.port, host = "0.0.0.0")
        .withHttpApp(Logger(logHeaders = true, logBody = true)(routes))
        .serve
        .compile
        .lastOrError
    }
  }
}
