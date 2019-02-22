package org.broadinstitute.transporter

import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.implicits._
import doobie.util.ExecutionContexts
import org.broadinstitute.transporter.web.{InfoRoutes, SwaggerMiddleware, WebConfig}
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaClient
import org.broadinstitute.transporter.info.InfoController
import org.http4s.HttpApp
import org.http4s.implicits._
import org.http4s.rho.swagger.models.Info
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.Logger
import pureconfig.module.catseffect._

/**
  * Main entry-point for the Transporter web service.
  *
  * Important to note: the [[IOApp]] wrapper handles initializing important
  * things like a global [[scala.concurrent.ExecutionContext]] with corresponding
  * [[cats.effect.ContextShift]] and [[cats.effect.Timer]].
  */
object TransporterManager extends IOApp {

  /** Top-level info to report about the app in its auto-generated documentation. */
  private val appInfo = Info(
    title = "Transporter API",
    version = BuildInfo.version,
    description = Some("Bulk file-transfer system for data ingest / delivery")
  )

  /** Type-fu version of `main`. */
  override def run(args: List[String]): IO[ExitCode] =
    loadConfigF[IO, ManagerConfig]("org.broadinstitute.transporter").flatMap { config =>
      val appResource = for {
        app <- buildApp(config)
      } yield {
        app
      }

      appResource.use(bindAndRun(_, config.web))
    }

  /**
    * Construct a Transporter web service which can be run by http4s.
    *
    * The constructed app is returned in a wrapper which will handle
    * setup / teardown logic for the underlying thread pools & external
    * connections used by the app.
    */
  private def buildApp(config: ManagerConfig): Resource[IO, HttpApp[IO]] =
    for {
      // Set up a thread pool to run all blocking I/O throughout the app.
      blockingEc <- ExecutionContexts.cachedThreadPool[IO]
      // Build clients for interacting with external resources, for use
      // across controllers in the app.
      dbResource = DbClient.resource(config.db, blockingEc)
      kafkaResource = KafkaClient.resource(config.kafka, blockingEc)
      (dbClient, kafkaClient) <- (dbResource, kafkaResource).tupled
    } yield {
      val appApi = new InfoRoutes(
        infoController = new InfoController(appInfo.version, dbClient, kafkaClient)
      )
      val appRoutes = SwaggerMiddleware(appApi, appInfo, blockingEc).orNotFound
      Logger.httpApp(logHeaders = true, logBody = true)(appRoutes)
    }

  /**
    * Run the given web service on a local port (specified by configuration).
    *
    * NOTE: This method only returns when a cancellation / interruption signal is
    * received. It should never produce an `IO(ExitCode.Success)`.
    */
  private def bindAndRun(app: HttpApp[IO], config: WebConfig): IO[ExitCode] =
    BlazeServerBuilder[IO]
      .bindHttp(port = config.port, host = config.host)
      .withHttpApp(app)
      .serve
      .compile
      .lastOrError
}
