package org.broadinstitute.transporter

import java.util.concurrent.{ExecutorService, Executors}

import cats.effect.{IO, Resource}
import org.broadinstitute.transporter.config.RunnerConfig
import org.broadinstitute.transporter.transfer.{AwsToGcpRunner, TransferRunner}

import scala.concurrent.ExecutionContext

/**
  * Transporter agent which can copy files from S3 to GCS, optionally
  * enforcing an expected length/md5 in the process.
  */
object AwsToGcpAgent extends TransporterAgent[RunnerConfig] {

  /** Build a resource wrapping a single-threaded execution context. */
  private def singleThreadedEc: Resource[IO, ExecutionContext] = {
    val allocate = IO.delay(Executors.newSingleThreadScheduledExecutor())
    val free = (es: ExecutorService) => IO.delay(es.shutdown())
    Resource.make(allocate)(free).map(ExecutionContext.fromExecutor)
  }

  override def runnerResource(config: RunnerConfig): Resource[IO, TransferRunner] =
    for {
      ec <- singleThreadedEc
      runner <- AwsToGcpRunner.resource(config, ec)
    } yield {
      runner
    }
}
