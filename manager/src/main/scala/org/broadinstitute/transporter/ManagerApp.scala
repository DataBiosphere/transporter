package org.broadinstitute.transporter

import cats.data.NonEmptyList
import cats.effect.{Clock, ContextShift, ExitCode, IO, Resource, Timer}
import cats.implicits._
import doobie.util.ExecutionContexts
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.info.InfoController
import org.broadinstitute.transporter.kafka.AdminClient
import org.broadinstitute.transporter.queue.QueueController
import org.broadinstitute.transporter.transfer.{
  ResultListener,
  SubmissionSweeper,
  TransferController
}
import org.broadinstitute.transporter.web.{ApiRoutes, InfoRoutes, SwaggerMiddleware}
import org.http4s.implicits._
import org.http4s.rho.swagger.models.Info
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.Logger

/** Components defining the Transporter Manager service. */
class ManagerApp private (
  webServer: BlazeServerBuilder[IO],
  submissionSweeper: SubmissionSweeper,
  resultListener: ResultListener
)(implicit cs: ContextShift[IO]) {

  /**
    * Run all components of the service.
    *
    * NOTE: In normal operation, this method will only return if the JVM is sent
    * a signal which the IOApp platform interprets to cancel the running IOs.
    */
  def run: IO[ExitCode] =
    (
      webServer.serve.compile.lastOrError,
      submissionSweeper.runSweeper,
      resultListener.processResults
    ).parMapN {
      case (exit, _, _) => exit
    }
}

object ManagerApp {

  /**
    * Construct an instance of the Manager service.
    *
    * The constructed service is returned in a wrapper which will handle
    * setup / teardown logic for the underlying clients used by the app.
    */
  def resource(config: ManagerConfig, info: Info)(
    implicit cs: ContextShift[IO],
    clk: Clock[IO],
    t: Timer[IO]
  ): Resource[IO, ManagerApp] =
    for {
      // Set up a thread pool to run all blocking I/O throughout the app.
      blockingEc <- ExecutionContexts.cachedThreadPool[IO]
      // Build clients for interacting with external resources, for use
      // across controllers in the app.
      dbClient <- DbClient.resource(config.db, blockingEc)
      adminClient <- AdminClient.resource(config.kafka, blockingEc)
      sweeper <- SubmissionSweeper.resource(dbClient, config)
      listener <- ResultListener.resource(dbClient, config.kafka)
    } yield {
      val queueController = QueueController(dbClient, adminClient)
      val routes = NonEmptyList.of(
        new InfoRoutes(new InfoController(info.version, dbClient, adminClient)),
        new ApiRoutes(
          queueController,
          TransferController(queueController, dbClient)
        )
      )
      val appRoutes = SwaggerMiddleware(routes, info, blockingEc).orNotFound
      val http = Logger.httpApp(logHeaders = true, logBody = true)(appRoutes)

      val server = BlazeServerBuilder[IO]
        .bindHttp(port = config.web.port, host = config.web.host)
        .withHttpApp(http)

      new ManagerApp(server, sweeper, listener)
    }
}
