package org.broadinstitute.transporter

import java.util.concurrent.{ExecutorService, Executors}

import cats.effect.{IO, Resource}
import org.broadinstitute.transporter.api.{
  SftpToGcsOutput => Out,
  SftpToGcsProgress => Progress,
  SftpToGcsRequest => In
}
import org.broadinstitute.transporter.config.RunnerConfig
import org.broadinstitute.transporter.transfer.{SftpToGcsRunner, TransferRunner}

import scala.concurrent.ExecutionContext

object SftpToGcsAgent extends TransporterAgent[RunnerConfig, In, Progress, Out] {

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
      runner <- SftpToGcsRunner.resource(config, ec)
    } yield {
      runner
    }
}
