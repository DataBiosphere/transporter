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

/**
  * Transfer runner which can copy files from S3 to GCS, optionally
  * enforcing an expected length/md5 in the process.
  *
  * Runs S3 download and GCS upload processes concurrently, with a
  * bounded buffer in-between.
  *
  * @param s3 client which can run operations against S3
  * @param s3Ec thread pool which should run all requests to S3
  * @param gcs client which can run operations against GCS
  * @param gcsEc thread pool which should run all requests go GCS
  * @param cs utility which can shift operations between threads
  * @param t utility which can schedule operations for later execution
  */
class AwsToGcpRunner(
  s3: S3Client,
  s3Ec: ExecutionContext,
  gcs: Storage,
  gcsEc: ExecutionContext
)(
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
      TransferSummary(TransferResult.Success, None)
    }
  }

  /**
    * Copy an object from S3 to GCS.
    *
    * @param s3Builder request-builder pointing at the S3 object to copy
    * @param gcsWriter write-channel pointing at the GCS object to create
    * @param fileSize size in bytes of the object to copy
    */
  private def runTransfer(
    s3Builder: GetObjectRequest.Builder,
    gcsWriter: WriteChannel,
    fileSize: Long
  ): IO[Unit] =
    for {
      buffer <- Queue.bounded[IO, Byte](BufferSize)
      s3Pull = s3MultipartDownload(s3Builder, fileSize).through(buffer.enqueue)
      gcsPush = buffer.dequeue.through(writeGcsBytes(gcsWriter, fileSize))
      // Order matters here: the argument to `concurrently` is considered the 'background'
      // stream, and won't interrupt processing if it completes first.
      _ <- gcsPush.concurrently(s3Pull).compile.drain
    } yield ()

  /**
    * Download the contents of an object in S3.
    *
    * @param reqBuilder request-builder pointing at the S3 object to download
    * @param fileSize size in bytes of the object to download
    */
  private def s3MultipartDownload(
    reqBuilder: GetObjectRequest.Builder,
    fileSize: Long
  ): Stream[IO, Byte] =
    Stream
      .unfold(0L) { start =>
        // First, build the content ranges of the requests to send to S3.
        // We do this instead of opening one mega-request to avoid tripping
        // any request timeouts.
        if (start == fileSize) {
          None
        } else {
          // The min is important; without it, S3 will return a 4XX error
          // if we specify a range that exceeds the total file size.
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

          fs2.io.readInputStream[IO](inStream, 8192, s3Ec)
      }

  /**
    * Build a stream sink which uploads bytes to an object in GCS.
    *
    * @param channel writer pointing to the GCS object to upload
    * @param fileSize expected size in bytes of the total upload
    */
  private def writeGcsBytes(channel: WriteChannel, fileSize: Long): Pipe[IO, Byte, Unit] =
    // 5 seconds is just guesswork here, could probably use some tuning.
    _.groupWithin(BytesPerMib, 5.seconds)
      .evalScan(0L) {
        case (numUploaded, byteChunk) =>
          val nextNum = numUploaded + byteChunk.size
          for {
            _ <- logger.info(
              s"Uploading bytes $numUploaded-${nextNum - 1} to GCS..."
            )
            _ <- cs.evalOn(gcsEc)(IO.delay(channel.write(byteChunk.toByteBuffer)))
          } yield nextNum
      }
      // This 'takeWhile' is very important; without it, the stream never
      // completes because the upstream byte buffer doesn't know when to close.
      .takeWhile(_ < fileSize)
      .map(_ => ())
}

object AwsToGcpRunner {

  val BytesPerMib: Int = math.pow(2, 20).toInt

  /** Number of bytes to request from S3 at a time. Probably needs tuning. */
  val S3RangeSize: Int = 5 * BytesPerMib

  /** Max number of bytes to store in-memory between the parallel download & upload streams. */
  val BufferSize: Int = 512 * BytesPerMib

  /** Exception raised when the size of the source S3 object doesn't match a request's expectations. */
  case class UnexpectedFileSize(uri: String, expected: Long, actual: Long)
      extends IllegalStateException(
        s"Object $uri has size $actual, but expected $expected"
      )
}
