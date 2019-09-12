package org.broadinstitute.transporter.transfer

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import org.broadinstitute.monster.storage.gcs.{GcsApi, JsonHttpGcsApi}
import org.broadinstitute.monster.storage.sftp.{SftpApi, SshjSftpApi}
import org.broadinstitute.transporter.api.{
  SftpToGcsOutput,
  SftpToGcsProgress,
  SftpToGcsRequest
}
import org.broadinstitute.transporter.config.RunnerConfig
import org.broadinstitute.transporter.kafka.{Done, Progress, TransferStep}
import org.http4s.MediaType
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.{Logger, Retry, RetryPolicy}
import org.http4s.headers._

import scala.concurrent.ExecutionContext

/**
  * Transfer runner which can copy files into GCS from an SFTP site.
  *
  * @param sftp client which can read data from a single SFTP site
  * @param gcs client which can write data into GCS
  */
class SftpToGcsRunner private[transfer] (sftp: SftpApi, gcs: GcsApi)
    extends TransferRunner[SftpToGcsRequest, SftpToGcsProgress, SftpToGcsOutput] {

  override def initialize(
    request: SftpToGcsRequest
  ): TransferStep[SftpToGcsRequest, SftpToGcsProgress, SftpToGcsOutput] =
    sftp
      .statFile(request.sftpPath)
      .flatMap {
        case None =>
          IO.raiseError(
            new RuntimeException(s"No SFTP file found at ${request.sftpPath}")
          )
        case Some(sourceAttrs) =>
          if (sourceAttrs.size < JsonHttpGcsApi.DefaultWriteChunkSize) {
            gcs
              .createObjectOneShot(
                request.gcsBucket,
                request.gcsPath,
                `Content-Type`(MediaType.application.`octet-stream`),
                None,
                sftp.readFile(request.sftpPath)
              )
              .as(Done(SftpToGcsOutput(request.gcsBucket, request.gcsPath)))
          } else {
            gcs
              .initResumableUpload(
                request.gcsBucket,
                request.gcsPath,
                `Content-Type`(MediaType.application.`octet-stream`),
                sourceAttrs.size,
                None
              )
              .map { token =>
                Progress(
                  SftpToGcsProgress(
                    request.sftpPath,
                    request.gcsBucket,
                    request.gcsPath,
                    token,
                    0L,
                    sourceAttrs.size
                  )
                )
              }
          }
      }
      .unsafeRunSync()

  override def step(
    progress: SftpToGcsProgress
  ): TransferStep[Nothing, SftpToGcsProgress, SftpToGcsOutput] = {
    val lastByte = math.min(
      progress.bytesUploaded + JsonHttpGcsApi.DefaultWriteChunkSize,
      progress.totalBytes
    )
    val byteSlice =
      sftp.readFile(progress.sftpPath, progress.bytesUploaded, Some(lastByte))
    gcs
      .uploadBytes(
        progress.gcsBucket,
        progress.gcsToken,
        progress.bytesUploaded,
        byteSlice
      )
      .map {
        case Left(bytesStored) => Progress(progress.copy(bytesUploaded = bytesStored))
        case Right(())         => Done(SftpToGcsOutput(progress.gcsBucket, progress.gcsPath))
      }
      .unsafeRunSync()
  }
}

object SftpToGcsRunner {

  def resource(config: RunnerConfig, ec: ExecutionContext)(
    implicit cs: ContextShift[IO],
    t: Timer[IO]
  ): Resource[IO, SftpToGcsRunner] = {
    val sftp = SshjSftpApi.build(
      config.sftp,
      ec,
      maxRetries = config.retries.maxRetries,
      retryDelay = config.retries.maxDelay
    )

    val gcs = BlazeClientBuilder[IO](ec)
      .withResponseHeaderTimeout(config.timeouts.responseHeaderTimeout)
      .withRequestTimeout(config.timeouts.requestTimeout)
      .resource
      .evalMap { httpClient =>
        val retryPolicy = RetryPolicy[IO](
          RetryPolicy
            .exponentialBackoff(config.retries.maxDelay, config.retries.maxRetries)
        )
        val retryingClient = Retry(retryPolicy)(httpClient)

        JsonHttpGcsApi.build(
          Logger(logHeaders = true, logBody = false)(retryingClient),
          config.gcsServiceAccount
        )
      }

    (sftp, gcs).mapN { case (s, g) => new SftpToGcsRunner(s, g) }
  }
}
