package org.broadinstitute.transporter

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

object AwsToGcpAgent extends TransporterAgent[RunnerConfig, AwsToGcpRequest] {

  override def runnerResource(
    config: RunnerConfig
  ): Resource[IO, TransferRunner[AwsToGcpRequest]] = {
    val create = (config.aws.toClient, config.gcp.toClient).tupled
    val close = (clients: (S3Client, Storage)) => IO.delay(clients._1.close())

    Resource.make(create)(close).map {
      case (s3, gcs) => new AwsToGcpRunner(s3, gcs)
    }
  }
}
