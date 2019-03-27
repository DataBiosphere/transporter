package org.broadinstitute.transporter

import java.util.concurrent.{ExecutorService, Executors}

import cats.effect.{IO, Resource}
import cats.implicits._
import com.google.cloud.storage.Storage
import org.broadinstitute.transporter.config.RunnerConfig
import org.broadinstitute.transporter.transfer.{
  AwsToGcpRequest,
  AwsToGcpRunner,
  TransferRunner
}
import software.amazon.awssdk.services.s3.S3Client

import scala.concurrent.ExecutionContext

/**
  * Transporter agent which can copy files from S3 to GCS, optionally
  * enforcing an expected length/md5 in the process.
  */
object AwsToGcpAgent extends TransporterAgent[RunnerConfig, AwsToGcpRequest] {

  /** Build a resource wrapping a single-threaded execution context. */
  private def singleThreadedEc: Resource[IO, ExecutionContext] = {
    val allocate = IO.delay(Executors.newSingleThreadScheduledExecutor())
    val free = (es: ExecutorService) => IO.delay(es.shutdown())
    Resource.make(allocate)(free).map(ExecutionContext.fromExecutor)
  }

  override def runnerResource(
    config: RunnerConfig
  ): Resource[IO, TransferRunner[AwsToGcpRequest]] =
    for {
      (s3Ec, gcsEc) <- (singleThreadedEc, singleThreadedEc).tupled
      create = (config.aws.toClient, config.gcp.toClient).tupled
      close = (clients: (S3Client, Storage)) =>
        contextShift.evalOn(s3Ec)(IO.delay(clients._1.close()))
      (s3, gcs) <- Resource.make(create)(close)
    } yield {
      new AwsToGcpRunner(s3, s3Ec, gcs, gcsEc)
    }
}
