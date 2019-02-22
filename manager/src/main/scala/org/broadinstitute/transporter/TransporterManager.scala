package org.broadinstitute.transporter

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.implicits._
import doobie.util.ExecutionContexts
import org.broadinstitute.transporter.web.{ManagerApi, SwaggerMiddleware}
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaClient
import org.broadinstitute.transporter.info.InfoController
import org.http4s.implicits._
import org.http4s.rho.swagger.models.Info
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.Logger
import pureconfig.module.catseffect._

import scala.concurrent.ExecutionContext

object TransporterManager extends IOApp {

  private val appInfo = Info(
    title = "Transporter API",
    version = BuildInfo.version,
    description = Some("Bulk file-transfer system for data ingest / delivery")
  )

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
    val kafkaResource = KafkaClient.resource(config.kafka, blockingEc)

    (dbResource, kafkaResource).tupled.use {
      case (dbClient, kafkaClient) =>
        val app = ManagerApp(
          infoController = new InfoController(appInfo.version, dbClient, kafkaClient)
        )

        val appApi = new ManagerApi(app)

        val routes = SwaggerMiddleware(appApi, appInfo, blockingEc).orNotFound

        BlazeServerBuilder[IO]
          .bindHttp(port = config.port, host = "0.0.0.0")
          .withHttpApp(Logger.httpApp(logHeaders = true, logBody = true)(routes))
          .serve
          .compile
          .lastOrError
    }
  }
}
