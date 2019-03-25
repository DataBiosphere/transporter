package org.broadinstitute.transporter.transfer

import cats.effect.{ContextShift, IO, Resource, Timer}
import com.google.cloud.WriteChannel
import com.google.cloud.storage.{BlobInfo, Storage}
import fs2.concurrent.Queue
import fs2.{Pipe, Stream}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, HeadObjectRequest}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class AwsToGcpRunner(s3: S3Client, gcs: Storage)(
  implicit cs: ContextShift[IO],
  t: Timer[IO]
) extends TransferRunner[AwsToGcpRequest] {
  import AwsToGcpRunner._

  private val logger = Slf4jLogger.getLogger[IO]

  override def transfer(request: AwsToGcpRequest): IO[TransferSummary] = {
    val s3Uri = s"s3://${request.s3Bucket}/${request.s3Path}"
    val gcsUri = s"gs://${request.gcsBucket}/${request.gcsPath}"

    for {
      _ <- logger.info(s"Fetching metadata for $s3Uri")
      s3Metadata <- IO.delay {
        s3.headObject(
          HeadObjectRequest
            .builder()
            .bucket(request.s3Bucket)
            .key(request.s3Path)
            .build()
        )
      }
      // We can sanity-check expected size up-front, but not md5, since multipart uploads
      // don't generate md5s for the final combined blob.
      s3Size = s3Metadata.contentLength()
      _ <- request.expectedSize.fold(
        logger.warn(s"No expected size given for $s3Uri, skipping sanity-check")
      ) { expectedSize =>
        if (s3Size == expectedSize) {
          logger.info(s"$s3Uri matches expected size; transferring to $gcsUri")
        } else {
          IO.raiseError(UnexpectedFileSize(s3Uri, expectedSize, s3Size))
        }
      }
      _ <- {
        val options = if (request.expectedMd5.isDefined) {
          List(Storage.BlobWriteOption.md5Match())
        } else {
          Nil
        }
        val gcsBase = BlobInfo.newBuilder(request.gcsBucket, request.gcsPath)
        val gcsTarget = request.expectedMd5
          .fold(gcsBase)(gcsBase.setMd5FromHexString)
          .setContentType(s3Metadata.contentType())
          .setContentEncoding(s3Metadata.contentEncoding())
          .setContentDisposition(s3Metadata.contentDisposition())
          .build()
        val s3Source =
          GetObjectRequest.builder().bucket(request.s3Bucket).key(request.s3Path)

        val open = IO.delay(gcs.writer(gcsTarget, options: _*))
        val close = (w: WriteChannel) => IO.delay(w.close())
        Resource.make(open)(close).use(runTransfer(s3Source, _, s3Size))
      }
    } yield {
      // FIXME: Handle errors & report meaningful info
      TransferSummary(TransferResult.Success, None)
    }
  }

  private def runTransfer(
    s3Builder: GetObjectRequest.Builder,
    gcsWriter: WriteChannel,
    fileSize: Long
  ): IO[Unit] =
    for {
      buffer <- Queue.bounded[IO, Byte](BufferSize)
      s3Pull = s3MultipartDownload(s3Builder, fileSize).through(buffer.enqueue)
      gcsPush = buffer.dequeue.through(writeBytes(gcsWriter, fileSize))
      // Order matters here: the argument to `concurrently` is considered the 'background'
      // stream, and won't interrupt processing if it completes first.
      _ <- gcsPush.concurrently(s3Pull).compile.drain
    } yield ()

  private def s3MultipartDownload(
    reqBuilder: GetObjectRequest.Builder,
    fileSize: Long
  ): Stream[IO, Byte] =
    Stream
      .unfold(0L) { start =>
        if (start == fileSize) {
          None
        } else {
          val newStart = math.min(start + S3RangeSize, fileSize)
          Some((start, newStart - 1) -> newStart)
        }
      }
      .flatMap {
        case (start, end) =>
          val range = s"$start-$end"
          val inStream = for {
            _ <- logger.info(s"Pulling bytes $range from S3...")
            respStream <- IO.delay(
              s3.getObject(reqBuilder.range(s"bytes=$range").build())
            )
            _ <- logger.debug(s"Got response for range $range")
          } yield {
            respStream
          }

          // FIXME: This shouldn't use the global EC.
          fs2.io.readInputStream[IO](inStream, 8192, ExecutionContext.global)
      }

  private def writeBytes(channel: WriteChannel, fileSize: Long): Pipe[IO, Byte, Unit] =
    _.groupWithin(BytesPerMib, 5.seconds)
      .evalScan(0L) {
        case (numUploaded, byteChunk) =>
          val nextNum = numUploaded + byteChunk.size
          for {
            _ <- logger.info(
              s"Uploading bytes $numUploaded-${nextNum - 1} to GCS..."
            )
            // FIXME: This shouldn't use the global EC.
            _ <- cs.evalOn(ExecutionContext.global)(
              IO.delay(channel.write(byteChunk.toByteBuffer))
            )
          } yield nextNum
      }
      .takeWhile(_ < fileSize)
      .map(_ => ())
}

object AwsToGcpRunner {
  val BytesPerMib: Int = math.pow(2, 20).toInt
  val S3RangeSize: Int = 5 * BytesPerMib
  val BufferSize: Int = 512 * BytesPerMib

  case class UnexpectedFileSize(uri: String, expected: Long, actual: Long)
      extends IllegalStateException(
        s"Object $uri has size $actual, but expected $expected"
      )
}
