package org.broadinstitute.transporter.transfer

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import fs2.{Chunk, Stream}
import fs2.concurrent.Queue
import org.apache.commons.codec.binary.Hex
import org.broadinstitute.monster.storage.gcs.{GcsApi, JsonHttpGcsApi}
import org.broadinstitute.transporter.config.RunnerConfig
import org.broadinstitute.transporter.kafka.{Done, Progress, TransferStep}
import org.broadinstitute.transporter.transfer.auth.S3AuthProvider
import org.http4s._
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.{Logger, Retry, RetryPolicy}
import org.http4s.headers._

import scala.concurrent.ExecutionContext

/**
  * Component which can transfer files from AWS to GCP, optionally checking the
  * expected size / md5 in the process.
  */
class AwsToGcpRunner(
  gcsClient: GcsApi,
  s3Client: (Request[IO], String) => Resource[IO, Response[IO]],
  bytesPerStep: Int
)(implicit cs: ContextShift[IO])
    extends TransferRunner[AwsToGcpRequest, AwsToGcpProgress, AwsToGcpOutput] {
  import AwsToGcpRunner._

  override def initialize(
    request: AwsToGcpRequest
  ): TransferStep[AwsToGcpRequest, AwsToGcpProgress, AwsToGcpOutput] = {
    val forceTransfer = request.force.getOrElse(false)

    val initFlow = for {
      // First, check if the source S3 object exists and is readable.
      s3Metadata <- getS3Metadata(
        bucket = request.s3Bucket,
        region = request.s3Region,
        path = request.s3Path
      )
      _ <- request.expectedSize.filter(_ != s3Metadata.contentLength).fold(IO.unit) {
        expectedSize =>
          IO.raiseError(
            UnexpectedFileSize(
              s"s3://${request.s3Bucket}/${request.s3Path}",
              expectedSize,
              s3Metadata.contentLength
            )
          )
      }

      /*
       * Second, check if there's already an object at the GCS target.
       *
       * If there is an existing object, and it has a registered md5 hash,
       * check if the md5 matches our expected final state. We use the
       * comparison to provide better UX for repeated transfers of the
       * same file.
       */
      gcsAttributes <- gcsClient.statObject(
        bucket = request.gcsBucket,
        path = request.gcsPath
      )
      gcsExists = gcsAttributes.isDefined
      md5sMatch = gcsAttributes.exists { attrs =>
        attrs.md5.isDefined && attrs.md5 == request.expectedMd5
      }

      /*
       * If there's already a GCS object and we're forcing the transfer,
       * delete the existing object.
       *
       * We do this up-front to force a fast failure in the case when the
       * agent is authorized to write objects, but not delete them, in the
       * target bucket. Without this fail-fast, the agent will happily
       * transfer bytes until the very last step of the resumable upload,
       * at which point Google will send a 403 when it tries to remove the
       * existing object.
       */
      _ <- gcsClient
        .deleteObject(bucket = request.gcsBucket, path = request.gcsPath)
        .whenA(gcsExists && forceTransfer)

      out <- if (md5sMatch && !forceTransfer) {
        // If there's already a GCS object with the md5 we want, we can short-circuit.
        IO.pure(Done(AwsToGcpOutput(request.gcsBucket, request.gcsPath)))
      } else if (gcsExists && !forceTransfer) {
        /*
         * If there's already a GCS object, but it doesn't have the md5 we expect,
         * we bail out with an error unless the "force" flag has been set.
         */
        IO.raiseError(
          GcsTargetTaken(
            s"gs://${request.gcsBucket}/${request.gcsPath}",
            gcsAttributes.flatMap(_.md5)
          )
        )
      } else if (s3Metadata.contentLength <= bytesPerStep) {
        // If the file is tiny, we can upload it in one shot.
        moveData(
          readS3Object(
            request.s3Bucket,
            request.s3Region,
            request.s3Path,
            rangeStart = 0,
            rangeEnd = s3Metadata.contentLength
          ),
          gcsClient.createObject(
            request.gcsBucket,
            request.gcsPath,
            s3Metadata.contentType.getOrElse(
              `Content-Type`(MediaType.application.`octet-stream`)
            ),
            s3Metadata.contentLength,
            request.expectedMd5,
            _
          )
        ).as(Done(AwsToGcpOutput(request.gcsBucket, request.gcsPath)))
      } else {
        // Finally, if we haven't found a reason to bail out early, we begin a GCS upload.
        gcsClient
          .initResumableUpload(
            bucket = request.gcsBucket,
            path = request.gcsPath,
            contentType = s3Metadata.contentType.getOrElse(
              `Content-Type`(MediaType.application.`octet-stream`)
            ),
            expectedSize = s3Metadata.contentLength,
            expectedMd5 = request.expectedMd5
          )
          .map { uploadToken =>
            Progress {
              AwsToGcpProgress(
                s3Bucket = request.s3Bucket,
                s3Region = request.s3Region,
                s3Path = request.s3Path,
                gcsBucket = request.gcsBucket,
                gcsPath = request.gcsPath,
                gcsToken = uploadToken,
                bytesUploaded = 0L,
                totalBytes = s3Metadata.contentLength
              )
            }
          }
      }
    } yield {
      out
    }

    initFlow.unsafeRunSync()
  }

  /**
    * Send a HEAD request to S3 to:
    *   1. Verify that an object exists, and
    *   2. Collect metadata about the file
    *
    * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
    */
  private def getS3Metadata(
    bucket: String,
    region: String,
    path: String
  ): IO[S3Metadata] = {
    val s3HttpUri = s3Uri(bucket, region, path)
    val s3Req = Request[IO](method = Method.HEAD, uri = s3HttpUri)

    s3Client(s3Req, region).use { response =>
      if (response.status.isSuccess) {
        response.contentLength.liftTo[IO](NoFileSize(s3HttpUri.renderString)).map {
          S3Metadata(_, response.contentType)
        }
      } else {
        S3Failure.raise(response, s"Failed to get metadata for $path in $bucket/$region")
      }
    }
  }

  override def step(
    progress: AwsToGcpProgress
  ): TransferStep[Nothing, AwsToGcpProgress, AwsToGcpOutput] =
    moveData(
      readS3Object(
        progress.s3Bucket,
        progress.s3Region,
        progress.s3Path,
        progress.bytesUploaded,
        // 'min' to avoid an "illegal range" error when pulling a 5MB chunk would
        // drive us over the total size of the file.
        math.min(progress.bytesUploaded + bytesPerStep, progress.totalBytes)
      ),
      gcsClient.uploadBytes(
        progress.gcsBucket,
        progress.gcsToken,
        progress.bytesUploaded,
        _
      )
    ).map {
      _.fold(
        bytesUploaded => Progress(progress.copy(bytesUploaded = bytesUploaded)),
        _ =>
          Done(AwsToGcpOutput(gcsBucket = progress.gcsBucket, gcsPath = progress.gcsPath))
      )
    }.unsafeRunSync()

  /**
    * Download a range of bytes from an object in S3.
    *
    * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
    */
  private def readS3Object(
    bucket: String,
    region: String,
    path: String,
    rangeStart: Long,
    rangeEnd: Long
  ): Stream[IO, Byte] = {
    if (rangeEnd <= rangeStart) {
      Stream.empty
    } else {
      val s3HttpUri = s3Uri(bucket, region, path)
      val readChunkSize = JsonHttpGcsApi.DefaultReadChunkSize

      def pullBytes(startByte: Long, endByte: Long): Stream[IO, Byte] = {
        val finalRequest = endByte - startByte - 1 <= readChunkSize
        val rangeEnd = if (finalRequest) endByte else startByte + readChunkSize
        val range = Range(startByte, rangeEnd - 1)

        val request = Request[IO](
          method = Method.GET,
          uri = s3HttpUri,
          headers = Headers.of(range)
        )

        val responseStream = Stream.resource(s3Client(request, region)).flatMap {
          response =>
            if (response.status.isSuccess) {
              response.body
            } else {
              Stream.eval(
                S3Failure.raise(
                  response,
                  s"Failed to get object bytes from $path in $bucket/$region"
                )
              )
            }
        }

        if (finalRequest) {
          responseStream
        } else {
          responseStream.append(pullBytes(rangeEnd, endByte))
        }
      }

      pullBytes(rangeStart, rangeEnd)
    }
  }

  /**
    * Copy a slice of data from S3 to GCS.
    *
    * Depending on configured chunk sizes, download / upload streams may
    * require making multiple round-trip requests to each storage system.
    * The transfer process runs both streams concurrently, with an in-memory
    * local buffer, to improve throughput.
    */
  private def moveData[O](
    downloadStream: Stream[IO, Byte],
    runUpload: Stream[IO, Byte] => IO[O]
  ): IO[O] =
    Queue.noneTerminated[IO, Chunk[Byte]].flatMap { buffer =>
      val runDownload = downloadStream.chunks
        .map(Some(_))
        .append(Stream.emit(None))
        .through(buffer.enqueue)
        .compile
        .drain

      (runDownload, runUpload(buffer.dequeue.flatMap(Stream.chunk)))
        .parMapN((_, out) => out)
    }
}

object AwsToGcpRunner {

  /**
    * Default region in AWS.
    *
    * HTTP requests to buckets in this region must use 's3' as their subdomain,
    * as opposed to the 's3-region' pattern used by everything else.
    */
  private[transfer] val DefaultAwsRegion = "us-east-1"

  /**
    * Characters which don't require URI encoding when included in HTTP paths to AWS.
    *
    * @see https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html#create-signature-presign-entire-payload
    */
  private[transfer] val awsReservedChars = ('A' to 'Z').toSet
    .union(('a' to 'z').toSet)
    .union(('0' to '9').toSet)
    .union(Set('_', '-', '~', '.'))

  /** Build the REST API endpoint for a bucket/path in S3. */
  private[transfer] def s3Uri(bucket: String, region: String, path: String): Uri = {
    val subdomain = if (region == DefaultAwsRegion) "s3" else s"s3-$region"
    val encoded = path
      .split('/')
      .map { segment =>
        segment.flatMap {
          case ch if awsReservedChars.contains(ch) => ch.toString
          case toEncode                            => s"%${Hex.encodeHexString(Array(toEncode.toByte), false)}"
        }
      }
      .mkString("/")
    Uri.unsafeFromString(s"https://$bucket.$subdomain.amazonaws.com/$encoded")
  }

  private val bytesPerMib = 1024 * 1024

  /** Construct an AWS -> GCP transfer runner, wrapped in setup/teardown logic for supporting clients. */
  def resource(config: RunnerConfig, ec: ExecutionContext)(
    implicit cs: ContextShift[IO],
    t: Timer[IO]
  ): Resource[IO, AwsToGcpRunner] =
    BlazeClientBuilder[IO](ec)
      .withResponseHeaderTimeout(config.timeouts.responseHeaderTimeout)
      .withRequestTimeout(config.timeouts.requestTimeout)
      .resource
      .map { baseClient =>
        val retryPolicy = RetryPolicy[IO](
          RetryPolicy
            .exponentialBackoff(config.retries.maxDelay, config.retries.maxRetries)
        )
        val retryingClient = Retry(retryPolicy)(baseClient)
        Logger(logHeaders = true, logBody = false)(retryingClient)
      }
      .evalMap { httpClient =>
        JsonHttpGcsApi.build(httpClient, config.gcp.serviceAccountJson).map { gcsClient =>
          val s3Auth = new S3AuthProvider(config.aws)
          new AwsToGcpRunner(
            gcsClient,
            (req, region) => {
              val headers = req.uri.authority
                .fold(req.headers) { authority =>
                  req.headers.put(Host(authority.host.renderString, authority.port))
                }
              Resource
                .liftF(s3Auth.addAuth(region, req.withHeaders(headers)))
                .flatMap(httpClient.run)
            },
            config.mibPerStep * bytesPerMib
          )
        }
      }

  /** Subset of metadata we pull from objects in S3 to initialize the uploads of GCS copies. */
  case class S3Metadata(contentLength: Long, contentType: Option[`Content-Type`])

  /**
    * Error raised when an object in S3 does not report a size.
    *
    * S3 objects should always have a known size, so if we get a response without that data
    * it's a sign that something went wrong in the request/response cycle.
    */
  case class NoFileSize(uri: String)
      extends IllegalStateException(s"Object $uri doesn't have a recorded size")

  /**
    * Error raised when the size of the source S3 object doesn't match a request's expectations.
    *
    * We use a size check as a fail-fast when initializing transfers.
    */
  case class UnexpectedFileSize(uri: String, expected: Long, actual: Long)
      extends IllegalStateException(
        s"Object $uri has size $actual, but expected $expected"
      )

  /** Error raised when a transfer would overwrite an existing object in GCS. */
  case class GcsTargetTaken(uri: String, md5: Option[String])
      extends IllegalStateException(s"$uri already exists in GCS${md5.fold("") { m =>
        s" with unexpected md5 '$m'"
      }}")
}
