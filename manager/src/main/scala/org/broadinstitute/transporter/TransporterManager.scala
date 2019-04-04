package org.broadinstitute.transporter

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import doobie.util.ExecutionContexts
import io.circe.Json
import org.broadinstitute.transporter.web.{
  ApiRoutes,
  InfoRoutes,
  SwaggerMiddleware,
  WebConfig
}
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.{
  AdminClient,
  KafkaConsumer,
  KafkaProducer,
  Serdes
}
import org.broadinstitute.transporter.info.InfoController
import org.broadinstitute.transporter.kafka.config.KafkaConfig
import org.broadinstitute.transporter.queue.QueueController
import org.broadinstitute.transporter.transfer.{
  ResultListener,
  TransferController,
  TransferSummary
}
import org.http4s.HttpApp
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
    for {
      config <- loadConfigF[IO, ManagerConfig]("org.broadinstitute.transporter")
      manager = new TransporterManager(config, appInfo)
      retcode <- manager.run
    } yield {
      retcode
    }
}

class TransporterManager private[transporter] (config: ManagerConfig, info: Info)(
  implicit cs: ContextShift[IO],
  t: Timer[IO]
) {

  def run: IO[ExitCode] = appResource(config).use {
    case (httpApp, listener) =>
      (bindAndRun(httpApp, config.web), listener.processResults).parMapN {
        case (exit, _) => exit
      }
  }

  /**
    * Construct a Transporter web service which can be run by http4s.
    *
    * The constructed app is returned in a wrapper which will handle
    * setup / teardown logic for the underlying thread pools & external
    * connections used by the app.
    */
  private def appResource(
    config: ManagerConfig
  ): Resource[IO, (HttpApp[IO], ResultListener)] =
    for {
      // Set up a thread pool to run all blocking I/O throughout the app.
      blockingEc <- ExecutionContexts.cachedThreadPool[IO]
      // Build clients for interacting with external resources, for use
      // across controllers in the app.
      dbClient <- DbClient.resource(config.db, blockingEc)
      adminClient <- AdminClient.resource(config.kafka, blockingEc)
      producer <- KafkaProducer.resource(
        config.kafka,
        Serdes.fuuidSerializer,
        Serdes.encodingSerializer[Json]
      )
      consumer <- KafkaConsumer.resource(
        s"${KafkaConfig.ResponseTopicPrefix}.+".r,
        config.kafka,
        Serdes.fuuidDeserializer,
        Serdes.decodingDeserializer[TransferSummary]
      )
    } yield {
      val queueController = QueueController(dbClient, adminClient)
      val routes = NonEmptyList.of(
        new InfoRoutes(new InfoController(info.version, dbClient, adminClient)),
        new ApiRoutes(
          queueController,
          TransferController(queueController, dbClient, producer)
        )
      )
      val appRoutes = SwaggerMiddleware(routes, info, blockingEc).orNotFound
      val http = Logger.httpApp(logHeaders = true, logBody = true)(appRoutes)
      val listener = ResultListener(consumer, producer, dbClient)

      (http, listener)
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
