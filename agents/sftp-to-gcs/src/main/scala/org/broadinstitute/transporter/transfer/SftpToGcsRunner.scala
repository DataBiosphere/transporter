package org.broadinstitute.transporter.transfer

import cats.effect.{Blocker, ContextShift, IO, Resource, Timer}
import cats.implicits._
import fs2.{Chunk, Stream}
import fs2.concurrent.Queue
import org.broadinstitute.monster.storage.common.{FileAttributes, FileType}
import org.broadinstitute.monster.storage.gcs.{GcsApi, JsonHttpGcsApi}
import org.broadinstitute.monster.storage.sftp.{SftpApi, SshjSftpApi}
import org.broadinstitute.transporter.api.{
  SftpToGcsOutput,
  SftpToGcsProgress,
  SftpToGcsRequest
}
import org.broadinstitute.transporter.config.RunnerConfig
import org.broadinstitute.transporter.kafka.{Done, Expanded, Progress, TransferStep}
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
class SftpToGcsRunner private[transfer] (sftp: SftpApi, gcs: GcsApi, bytesPerStep: Int)(
  implicit cs: ContextShift[IO]
) extends TransferRunner[SftpToGcsRequest, SftpToGcsProgress, SftpToGcsOutput] {

  override def initialize(
    request: SftpToGcsRequest
  ): TransferStep[SftpToGcsRequest, SftpToGcsProgress, SftpToGcsOutput] = {
    val result = if (request.isDirectory) {
      expandDirectoryRequest(request)
    } else {
      initFileRequest(request)
    }
    result.unsafeRunSync()
  }

  /**
    * Convert a request to recursively copy an SFTP directory into a list of requests
    * to copy each entry in the directory.
    */
  private def expandDirectoryRequest(
    request: SftpToGcsRequest
  ): IO[Expanded[SftpToGcsRequest]] =
    sftp
      .listDirectory(request.sftpPath)
      .map {
        case (path, fileType) =>
          SftpToGcsRequest(
            path,
            request.gcsBucket,
            request.gcsPath + path.substring(request.sftpPath.length),
            fileType == FileType.Directory
          )
      }
      .compile
      .toList
      .map(Expanded(_))

  /** Initialize the transfer of an SFTP file to GCS. */
  private def initFileRequest(
    request: SftpToGcsRequest
  ): IO[TransferStep[Nothing, SftpToGcsProgress, SftpToGcsOutput]] =
    sftp
      .statFile(request.sftpPath)
      .flatMap {
        case None =>
          IO.raiseError(
            new RuntimeException(s"No SFTP file found at ${request.sftpPath}")
          )
        case Some(sourceAttrs) =>
          if (sourceAttrs.size < bytesPerStep) {
            transferOneShot(request, sourceAttrs).as(
              Done(SftpToGcsOutput(request.gcsBucket, request.gcsPath))
            )
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

  private def transferOneShot(
    request: SftpToGcsRequest,
    sourceAttrs: FileAttributes
  ): IO[Unit] =
    Queue.noneTerminated[IO, Chunk[Byte]].flatMap { buffer =>
      val runDownload = sftp
        .readFile(request.sftpPath)
        .chunks
        .map(Some(_))
        .append(Stream.emit(None))
        .through(buffer.enqueue)
        .compile
        .drain

      val runUpload = gcs.createObject(
        request.gcsBucket,
        request.gcsPath,
        `Content-Type`(MediaType.application.`octet-stream`),
        sourceAttrs.size,
        None,
        buffer.dequeue.flatMap(Stream.chunk)
      )

      List(runDownload, runUpload).parSequence_
    }

  override def step(
    progress: SftpToGcsProgress
  ): TransferStep[Nothing, SftpToGcsProgress, SftpToGcsOutput] = {
    val lastByte = math.min(
      progress.bytesUploaded + bytesPerStep,
      progress.totalBytes
    )

    Queue
      .noneTerminated[IO, Chunk[Byte]]
      .flatMap { buffer =>
        val downloadSlice = sftp
          .readFile(progress.sftpPath, progress.bytesUploaded, Some(lastByte))
          .chunks
          .map(Some(_))
          .append(Stream.emit(None))
          .through(buffer.enqueue)
          .compile
          .drain

        val uploadSlice = gcs.uploadBytes(
          progress.gcsBucket,
          progress.gcsToken,
          progress.bytesUploaded,
          buffer.dequeue.flatMap(Stream.chunk)
        )

        (downloadSlice, uploadSlice).parMapN {
          case (_, Left(bytesStored)) =>
            Progress(progress.copy(bytesUploaded = bytesStored))
          case (_, Right(())) =>
            Done(SftpToGcsOutput(progress.gcsBucket, progress.gcsPath))
        }
      }
      .unsafeRunSync()
  }
}

object SftpToGcsRunner {

  private val bytesPerKib = 1024
  private val bytesPerMib = 1024 * bytesPerKib

  def resource(config: RunnerConfig, ec: ExecutionContext, blocker: Blocker)(
    implicit cs: ContextShift[IO],
    t: Timer[IO]
  ): Resource[IO, SftpToGcsRunner] = {
    val sftp = SshjSftpApi.build(
      config.sftp,
      blocker,
      readChunkSize = 256 * bytesPerKib,
      maxRetries = config.retries.maxRetries,
      retryDelay = config.retries.maxDelay,
      readAhead = config.maxConcurrentReads
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
          config.gcsServiceAccount,
          writeChunkSize = bytesPerMib
        )
      }

    (sftp, gcs).mapN {
      case (s, g) => new SftpToGcsRunner(s, g, config.mibPerStep * bytesPerMib)
    }
  }
}
