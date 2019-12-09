package org.broadinstitute.transporter

import cats.effect._
import doobie.util.ExecutionContexts
import org.broadinstitute.monster.TransporterManagerBuildInfo
import org.broadinstitute.transporter.db.DbTransactor
import org.broadinstitute.transporter.info.InfoController
import org.broadinstitute.transporter.transfer.{
  TransferController,
  TransferListener,
  TransferSubmitter
}
import org.broadinstitute.transporter.web.WebApi
import org.http4s.server.blaze.BlazeServerBuilder
import pureconfig.ConfigSource
import pureconfig.module.catseffect._

import scala.concurrent.ExecutionContext

/**
  * Main entry-point for the Transporter web service.
  *
  * Important to note: the [[IOApp]] wrapper handles initializing important
  * things like a global [[scala.concurrent.ExecutionContext]] with corresponding
  * [[cats.effect.ContextShift]] and [[cats.effect.Timer]].
  */
object TransporterManager extends IOApp.WithContext {

  // Use a fixed-size thread pool w/ one thread per core for CPU-bound and non-blocking I/O.
  override val executionContextResource: Resource[SyncIO, ExecutionContext] =
    ExecutionContexts.fixedThreadPool[SyncIO](Runtime.getRuntime.availableProcessors)

  /** [[IOApp]] equivalent of `main`. */
  override def run(args: List[String]): IO[ExitCode] =
    ConfigSource.default
      .at("org.broadinstitute.transporter")
      .loadF[IO, ManagerConfig]
      .flatMap { config =>
        val app = for {
          // Set up a thread pool to run all blocking I/O throughout the app.
          blockingEc <- Blocker[IO]
          // Build clients for interacting with external resources, for use
          // across controllers in the app.
          transactor <- DbTransactor.resource(config.db, blockingEc)
          submitter <- TransferSubmitter.resource(
            transactor,
            config.transfer,
            config.kafka
          )
          controller = new TransferController(config.transfer.schema, transactor)
          listener <- TransferListener.resource(transactor, config.kafka, controller)
        } yield {
          val appRoutes = new WebApi(
            new InfoController(TransporterManagerBuildInfo.version, transactor),
            controller,
            googleAuthConfig = config.web.googleOauth,
            blockingEc = blockingEc,
            transferSchema = config.transfer.schema
          )

          val server = BlazeServerBuilder[IO]
            .bindHttp(port = config.web.port, host = config.web.host)
            .withResponseHeaderTimeout(config.web.responseTimeout)
            .withHttpApp(appRoutes.app)

          server.serve
            .concurrently(submitter.sweepSubmissions)
            .concurrently(listener.listen)
            .compile
        }

        app.use(_.lastOrError)
      }
}
