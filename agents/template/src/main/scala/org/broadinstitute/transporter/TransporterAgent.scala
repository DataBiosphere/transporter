package org.broadinstitute.transporter

import java.util.concurrent.{ExecutorService, Executors}

import cats.effect._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.streams.KafkaStreams
import org.broadinstitute.transporter.info.InfoController
import org.broadinstitute.transporter.kafka.{KStreamsConfig, TransferStreamBuilder}
import org.broadinstitute.transporter.transfer.TransferRunner
import org.broadinstitute.transporter.web.WebApi
import org.broadinstitute.transporter.web.config.WebConfig
import org.http4s.server.blaze.BlazeServerBuilder
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.module.catseffect._

import scala.concurrent.ExecutionContext

/**
  * Main entry-point for Transporter agent programs.
  *
  * Extending this base class should provide concrete agent programs with
  * the full framework needed to hook into Transporter's Kafka infrastructure
  * and run data transfers.
  */
abstract class TransporterAgent[
  Config: ConfigReader,
  In: Decoder,
  Progress: Encoder: Decoder,
  Out: Encoder
] extends IOApp.WithContext {

  // Use a single thread for the streams run loop.
  // Kafka Streams is an inherently synchronous API, so there's no point in
  // keeping multiple threads around.
  override val executionContextResource: Resource[SyncIO, ExecutionContext] = {
    val allocate = SyncIO(Executors.newSingleThreadExecutor())
    val free = (es: ExecutorService) => SyncIO(es.shutdown())
    Resource.make(allocate)(free).map(ExecutionContext.fromExecutor)
  }

  /**
    * Construct the agent component which can actually run data transfers.
    *
    * Modeled as a `Resource` so agent programs can hook in setup / teardown
    * logic for config, thread pools, etc.
    */
  def runnerResource(config: Config): Resource[IO, TransferRunner[In, Progress, Out]]

  /** [[IOApp]] equivalent of `main`. */
  final override def run(args: List[String]): IO[ExitCode] =
    ConfigSource.default
      .at("org.broadinstitute.transporter")
      .loadF[IO, AgentConfig[Config]]
      .flatMap { config =>
        runnerResource(config.runnerConfig).flatMap(streamResource(_, config.kafka)).use {
          stream =>
            runWebApi(new InfoController(stream), config.web)
        }
      }

  /**
    * Build a resource which will construct and launch a Kafka stream to
    * receive, process, and report on data transfer requests.
    */
  private def streamResource(
    runner: TransferRunner[In, Progress, Out],
    kafkaConfig: KStreamsConfig
  ): Resource[IO, KafkaStreams] = {
    val setup = for {
      topology <- new TransferStreamBuilder(kafkaConfig.topics, runner).build
      stream <- IO.delay(new KafkaStreams(topology, kafkaConfig.asProperties))
      _ <- IO.delay(stream.start())
    } yield {
      stream
    }
    Resource.make(setup)(stream => IO.delay(stream.close()))
  }

  /**
    * Run a web server for REST queries to the agent.
    *
    * The returned `IO` will only complete if the process receives a stop/interrupt
    * signal from the OS.
    */
  private def runWebApi(
    controller: InfoController,
    config: WebConfig
  ): IO[ExitCode] = {
    val api = new WebApi(controller)

    BlazeServerBuilder[IO]
      .withHttpApp(api.app)
      .bindHttp(host = config.host, port = config.port)
      .serve
      .compile
      .lastOrError
  }
}
