package org.broadinstitute.transporter

import cats.effect._
import cats.implicits._
import doobie.util.ExecutionContexts
import fs2.Stream
import io.circe.Json
import org.broadinstitute.transporter.db.DbTransactor
import org.broadinstitute.transporter.info.InfoController
import org.broadinstitute.transporter.kafka.{
  KafkaAdminClient,
  KafkaConsumer,
  KafkaProducer,
  TopicApi
}
import org.broadinstitute.transporter.queue.QueueController
import org.broadinstitute.transporter.transfer.{TransferController, TransferResult}
import org.broadinstitute.transporter.web.{ApiRoutes, InfoRoutes, SwaggerMiddleware}
import org.http4s.implicits._
import org.http4s.rho.swagger.models.Info
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.server.middleware.Logger
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

  /** Top-level info to report about the app in its auto-generated documentation. */
  private val appInfo = Info(
    title = "Transporter API",
    version = BuildInfo.version,
    description = Some("Bulk file-transfer system for data ingest / delivery")
  )

  /** [[IOApp]] equivalent of `main`. */
  override def run(args: List[String]): IO[ExitCode] =
    loadConfigF[IO, ManagerConfig]("org.broadinstitute.transporter").flatMap { config =>
      import org.broadinstitute.transporter.kafka.Serdes._

      val components = for {
        // Set up a thread pool to run all blocking I/O throughout the app.
        blockingEc <- ExecutionContexts.cachedThreadPool[IO]
        // Build clients for interacting with external resources, for use
        // across controllers in the app.
        transactor <- DbTransactor.resource(config.db, blockingEc)
        admin <- KafkaAdminClient.resource(
          config.kafka.connection,
          config.kafka.admin,
          blockingEc
        )
        producer <- KafkaProducer.resource[Json](config.kafka.connection)
        progressConsumer <- KafkaConsumer
          .ofTopicPattern[Json](TopicApi.ProgressSubscriptionPattern)
          .apply(config.kafka.connection, config.kafka.consumer)
        resultConsumer <- KafkaConsumer
          .ofTopicPattern[(TransferResult, Json)](TopicApi.ResponseSubscriptionPattern)
          .apply(config.kafka.connection, config.kafka.consumer)
      } yield {
        val transferController = new TransferController(transactor, producer)
        val appRoutes = SwaggerMiddleware(
          headerInfo = appInfo,
          unauthedRoutes =
            new InfoRoutes(new InfoController(appInfo.version, transactor, admin)),
          apiRoutes = new ApiRoutes(
            new QueueController(transactor, admin),
            transferController,
            config.web.googleOauth.isDefined
          ),
          googleAuthConfig = config.web.googleOauth,
          blockingEc = blockingEc
        ).orNotFound
        val http = Logger.httpApp(logHeaders = true, logBody = true)(appRoutes)

        val server = BlazeServerBuilder[IO]
          .bindHttp(port = config.web.port, host = config.web.host)
          .withHttpApp(http)
          .serve
          .compile
          .lastOrError

        val submissionSweeper = Stream
          .fixedDelay(config.submissionInterval)
          .evalMap(_ => transferController.submitEligibleTransfers)
          .compile
          .drain

        val progressListener =
          progressConsumer.runForeach(transferController.markTransfersInProgress)
        val resultListener =
          resultConsumer.runForeach(transferController.recordTransferResults)

        (server, submissionSweeper, progressListener, resultListener)
      }

      components.use(_.parTupled.map(_._1))
    }
}
