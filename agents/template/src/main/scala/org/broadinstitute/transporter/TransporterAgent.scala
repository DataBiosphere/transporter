package org.broadinstitute.transporter

import java.util.concurrent.{ExecutorService, Executors}

import cats.effect._
import io.circe.{Decoder, Encoder}
import org.apache.kafka.streams.KafkaStreams
import org.broadinstitute.transporter.kafka.{KStreamsConfig, TransferStreamBuilder}
import org.broadinstitute.transporter.transfer.TransferRunner
import pureconfig.ConfigReader
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
    loadConfigF[IO, AgentConfig[Config]]("org.broadinstitute.transporter").flatMap {
      config =>
        runnerResource(config.runnerConfig).use(runStream(_, config.kafka))
    }

  /**
    * Build and launch the Kafka stream which receives, processes, and reports
    * on data transfer requests.
    *
    * This method should only return if the underlying stream is interrupted
    * (i.e. by a Ctrl-C).
    */
  private def runStream(
    runner: TransferRunner[In, Progress, Out],
    kafkaConfig: KStreamsConfig
  ): IO[ExitCode] =
    for {
      topology <- new TransferStreamBuilder(kafkaConfig.topics, runner).build
      stream <- IO.delay(new KafkaStreams(topology, kafkaConfig.asProperties))
      _ <- IO.delay(stream.start())
      _ <- IO.cancelable[Unit](_ => IO.delay(stream.close()))
    } yield {
      ExitCode.Success
    }
}
