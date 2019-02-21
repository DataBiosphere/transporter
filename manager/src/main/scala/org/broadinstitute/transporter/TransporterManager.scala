package org.broadinstitute.transporter

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.implicits._
import doobie.util.ExecutionContexts
import org.broadinstitute.transporter.web.{ManagerApi, SwaggerMiddleware}
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaClient
import org.broadinstitute.transporter.status.StatusController
import org.http4s.implicits._
import org.http4s.rho.swagger.models.Info
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
    val dbResource = DbClient.resource(config.db, blockingEc)
    val kafkaResource = KafkaClient.resource(config.kafka)

    (dbResource, kafkaResource).tupled.use {
      case (dbClient, kafkaClient) =>
        val app = ManagerApp(
          statusController = new StatusController(dbClient, kafkaClient)
        )

        val appApi = new ManagerApi(BuildInfo.version, app)
        val appInfo = Info(
          title = "Transporter API",
          version = BuildInfo.version,
          description = Some("Bulk file-transfer system for data ingest / delivery")
        )

        val routes = SwaggerMiddleware(appApi, appInfo, blockingEc).orNotFound

        BlazeServerBuilder[IO]
          .bindHttp(port = config.port, host = "0.0.0.0")
          .withHttpApp(Logger(logHeaders = true, logBody = true)(routes))
          .serve
          .compile
          .lastOrError
    }
  }
}
