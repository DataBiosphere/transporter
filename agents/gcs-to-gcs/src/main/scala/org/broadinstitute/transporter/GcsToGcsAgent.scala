package org.broadinstitute.transporter

import java.util.concurrent.{ExecutorService, Executors}

import cats.effect.{IO, Resource}
import org.broadinstitute.transporter.api.{
  GcsToGcsOutput => Out,
  GcsToGcsProgress => Progress,
  GcsToGcsRequest => In
}
import org.broadinstitute.transporter.config.RunnerConfig
import org.broadinstitute.transporter.transfer.{GcsToGcsRunner, TransferRunner}

import scala.concurrent.ExecutionContext

/** Transporter agent which can copy files from one location in GCS to another. */
object GcsToGcsAgent extends TransporterAgent[RunnerConfig, In, Progress, Out] {

  /** Build a resource wrapping a single-threaded execution context. */
  private def singleThreadedEc: Resource[IO, ExecutionContext] = {
    val allocate = IO.delay(Executors.newSingleThreadScheduledExecutor())
    val free = (es: ExecutorService) => IO.delay(es.shutdown())
    Resource.make(allocate)(free).map(ExecutionContext.fromExecutor)
  }

  override def runnerResource(
    config: RunnerConfig
  ): Resource[IO, TransferRunner[In, Progress, Out]] =
    for {
      ec <- singleThreadedEc
      runner <- GcsToGcsRunner.resource(config, ec)
    } yield {
      runner
    }
}
