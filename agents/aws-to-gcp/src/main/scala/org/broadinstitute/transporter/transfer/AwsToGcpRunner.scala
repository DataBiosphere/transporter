package org.broadinstitute.transporter.transfer

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import fs2.{Chunk, Stream}
import io.circe.JsonObject
import io.circe.jawn.JawnParser
import io.circe.syntax._
import org.apache.commons.codec.binary.{Base64, Hex}
import org.broadinstitute.transporter.config.RunnerConfig
import org.broadinstitute.transporter.transfer.auth.{GcsAuthProvider, S3AuthProvider}
import org.http4s._
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.{Logger, Retry, RetryPolicy}
import org.http4s.headers._
import org.http4s.util.CaseInsensitiveString

import scala.concurrent.ExecutionContext

/**
  * Component which can transfer files from AWS to GCP, optionally checking the
  * expected size / md5 in the process.
  *
  * TODO: Consider using a synchronous HTTP client here instead of http4s, since
  * the Kafka APIs are synchronous and we end up `unsafeRunSync`-ing everywhere anyways.
  *
  * @param httpClient client to use when making requests to both AWS and GCP
  * @param googleAuth component which can add authentication to GCP requests
  * @param s3Auth component which can add authentication to AWS requests
  */
class AwsToGcpRunner(
  httpClient: Client[IO],
  googleAuth: GcsAuthProvider,
  s3Auth: S3AuthProvider
) extends TransferRunner[AwsToGcpRequest, AwsToGcpProgress, AwsToGcpOutput] {
  import AwsToGcpRunner._

  override def initialize(
    request: AwsToGcpRequest
  ): Either[AwsToGcpProgress, AwsToGcpOutput] = {

    val forceTransfer = request.force.getOrElse(false)

    val initFlow = for {

      // First, check if the source S3 object exists and is readable.
      s3Metadata <- getS3Metadata(
        bucket = request.s3Bucket,
        region = request.s3Region,
        path = request.s3Path,
        expectedSize = request.expectedSize
      )

      /*
       * Second, check if there's already an object at the GCS target.
       *
       * If there is an existing object, and it has a registered md5 hash,
       * check if the md5 matches our expected final state. We use the
       * comparison to provide better UX for repeated transfers of the
       * same file.
       */
      (gcsExists, existingMd5) <- checkExistingObject(
        bucket = request.gcsBucket,
        path = request.gcsPath
      )
      md5sMatch = existingMd5.isDefined && existingMd5 == request.expectedMd5

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
      _ <- deleteGcsObject(bucket = request.gcsBucket, path = request.gcsPath)
        .whenA(gcsExists && !md5sMatch && forceTransfer)

      out <- if (md5sMatch) {
        // If there's already a GCS object with the md5 we want, we can short-circuit.
        IO.pure(Right(AwsToGcpOutput(request.gcsBucket, request.gcsPath)))
      } else if (gcsExists && !forceTransfer) {
        /*
         * If there's already a GCS object, but it doesn't have the md5 we expect,
         * we bail out with an error unless the "force" flag has been set.
         */
        IO.raiseError(
          GcsTargetTaken(s"gs://${request.gcsBucket}/${request.gcsPath}", existingMd5)
        )
      } else {
        // Finally, if we haven't found a reason to bail out early, we begin a GCS upload.
        initGcsResumableUpload(
          bucket = request.gcsBucket,
          path = request.gcsPath,
          s3Metadata = s3Metadata,
          expectedMd5 = request.expectedMd5
        ).map { uploadToken =>
          Left {
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
    * Send a HEAD request to S3 to verify:
    *   1. That an object exists, and
    *   2. That the object's size matches our expectations, if any.
    *
    * S3 reliably generates Content-Length headers, so we can run that
    * check as an early fail-fast mechanism. Ideally we'd be able to do
    * the same thing with md5s, but S3 doesn't generate those for
    * multipart uploads.
    *
    * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
    */
  private def getS3Metadata(
    bucket: String,
    region: String,
    path: String,
    expectedSize: Option[Long]
  ): IO[S3Metadata] = {
    val s3HttpUri = s3Uri(bucket, region, path)

    for {
      s3Req <- IO.delay {
        Request[IO](
          method = Method.HEAD,
          uri = s3HttpUri,
          headers = s3HttpUri.authority
            .fold(Headers.empty) { authority =>
              Headers.of(Host(authority.host.renderString, authority.port))
            }
        )
      }
      metadata <- httpClient.fetch(s3Auth.addAuth(region, s3Req)) { response =>
        if (response.status.isSuccess) {
          response.contentLength.liftTo[IO](NoFileSize(s3HttpUri.renderString)).map {
            S3Metadata(_, response.contentType)
          }
        } else {
          IO.raiseError(
            new RuntimeException(
              s"Request for metadata from $s3HttpUri returned status ${response.status}"
            )
          )
        }
      }
      _ <- expectedSize.filter(_ != metadata.contentLength).fold(IO.unit) {
        expectedSize =>
          IO.raiseError(
            UnexpectedFileSize(
              s3HttpUri.renderString,
              expectedSize,
              metadata.contentLength
            )
          )
      }
    } yield {
      metadata
    }
  }

  private val parser = new JawnParser()

  /**
    * Query the URI of an object in GCS to:
    *   1. Check its existence, and
    *   2. Collect its reported md5, if any
    */
  private def checkExistingObject(
    bucket: String,
    path: String
  ): IO[(Boolean, Option[String])] = {
    val gcsUri = baseGcsUri(bucket, path)

    for {
      gcsReq <- IO.delay(Request[IO](method = Method.GET, uri = gcsUri))
      existingInfo <- httpClient.fetch(googleAuth.addAuth(gcsReq)) { response =>
        if (response.status == Status.NotFound) {
          IO.pure((false, None))
        } else if (response.status.isSuccess) {
          for {
            byteChunk <- response.body.compile.toChunk
            objectMetadata <- parser
              .parseByteBuffer(byteChunk.toByteBuffer)
              .flatMap(_.as[JsonObject])
              .liftTo[IO]
          } yield {
            (true, objectMetadata("md5Hash").flatMap(_.asString))
          }
        } else {
          IO.raiseError(
            new RuntimeException(
              s"Request for metadata from $gcsUri returned status ${response.status}"
            )
          )
        }
      }
    } yield {
      existingInfo
    }
  }

  /** Send a request to delete an object in GCS. */
  private def deleteGcsObject(bucket: String, path: String): IO[Unit] = {
    val gcsUri = baseGcsUri(bucket, path)

    for {
      gcsReq <- IO.delay(Request[IO](method = Method.DELETE, uri = gcsUri))
      deleteSuccessful <- httpClient.successful(googleAuth.addAuth(gcsReq))
      _ <- IO
        .raiseError(new RuntimeException(s"Failed to delete ${gcsUri.renderString}"))
        .whenA(!deleteSuccessful)
    } yield {
      ()
    }
  }

  /**
    * Initialize a GCS resumable upload using object metadata pulled from S3.
    *
    * If an expected md5 was provided with the transfer request, it will be included
    * as metadata in the upload-creation request. This will trigger server-side
    * content validation when all bytes are uploaded.
    *
    * @see https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload#start-resumable
    */
  private def initGcsResumableUpload(
    bucket: String,
    path: String,
    s3Metadata: S3Metadata,
    expectedMd5: Option[String]
  ): IO[String] = {
    // Object metadata is used by Google to register the upload to the correct pseudo-path.
    val baseObjectMetadata = JsonObject("name" -> path.asJson)
    val objectMetadata = expectedMd5
      .fold(baseObjectMetadata) { hexMd5 =>
        baseObjectMetadata.add(
          "md5Hash",
          Base64.encodeBase64String(Hex.decodeHex(hexMd5.toCharArray)).asJson
        )
      }
      .asJson

    val initBody = Stream.emits(objectMetadata.noSpaces.getBytes)

    for {
      initSize <- initBody.covary[IO].compile.fold(0L)((s, _) => s + 1)
      initHeaders = Headers.of(
        `Content-Length`.unsafeFromLong(initSize),
        `Content-Type`(MediaType.application.json, Charset.`UTF-8`),
        Header("X-Upload-Content-Length", s3Metadata.contentLength.toString)
      )
      gcsReq <- IO.delay {
        Request[IO](
          method = Method.POST,
          uri = baseGcsUploadUri(bucket),
          body = initBody,
          headers = s3Metadata.contentType.fold(initHeaders) { s3ContentType =>
            initHeaders.put(Header("X-Upload-Content-Type", s3ContentType.renderString))
          }
        )
      }
      locationHeader <- httpClient.fetch(googleAuth.addAuth(gcsReq)) { response =>
        response.headers
          .get(CaseInsensitiveString("X-GUploader-UploadID"))
          .liftTo[IO](NoToken(s"gs://$bucket/$path"))
      }
    } yield {
      locationHeader.value
    }

  }

  override def step(
    progress: AwsToGcpProgress
  ): Either[AwsToGcpProgress, AwsToGcpOutput] = {
    val doStep = for {
      chunk <- getS3Chunk(
        bucket = progress.s3Bucket,
        region = progress.s3Region,
        path = progress.s3Path,
        rangeStart = progress.bytesUploaded,
        // 'min' to avoid an "illegal range" error when pulling a 5MB chunk would
        // drive us over the total size of the file.
        rangeEnd = math.min(progress.bytesUploaded + ChunkSize, progress.totalBytes) - 1
      )
      nextOrDone <- uploadChunk(
        bucket = progress.gcsBucket,
        path = progress.gcsPath,
        uploadToken = progress.gcsToken,
        rangeStart = progress.bytesUploaded,
        chunk = chunk
      )
    } yield {
      nextOrDone.bimap(
        bytesUploaded => progress.copy(bytesUploaded = bytesUploaded),
        _ => AwsToGcpOutput(gcsBucket = progress.gcsBucket, gcsPath = progress.gcsPath)
      )
    }

    doStep.unsafeRunSync()
  }

  /**
    * Download a chunk of bytes from an object in S3.
    *
    * @see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
    */
  private def getS3Chunk(
    bucket: String,
    region: String,
    path: String,
    rangeStart: Long,
    rangeEnd: Long
  ): IO[Chunk[Byte]] = {
    val s3HttpUri = s3Uri(bucket, region, path)

    for {
      s3Req <- IO.delay {
        Request[IO](
          method = Method.GET,
          uri = s3HttpUri,
          headers = s3HttpUri.authority
            .fold(Headers.empty) { authority =>
              Headers.of(Host(authority.host.renderString, authority.port))
            }
            .put(Range(rangeStart, rangeEnd))
        )
      }
      chunk <- httpClient.expect[Chunk[Byte]](s3Auth.addAuth(region, s3Req))
    } yield {
      chunk
    }
  }

  /**
    * Upload a chunk of bytes to an ongoing GCS resumable upload.
    *
    * @see https://cloud.google.com/storage/docs/json_api/v1/how-tos/resumable-upload#upload-resumable
    */
  private def uploadChunk(
    bucket: String,
    path: String,
    uploadToken: String,
    rangeStart: Long,
    chunk: Chunk[Byte]
  ): IO[Either[Long, Unit]] =
    for {
      gcsReq <- IO.delay {
        Request[IO](
          method = Method.PUT,
          uri = baseGcsUploadUri(bucket).withQueryParam("upload_id", uploadToken),
          headers = Headers.of(
            `Content-Length`.unsafeFromLong(chunk.size.toLong),
            `Content-Range`(rangeStart, rangeStart + chunk.size - 1)
          ),
          body = Stream.chunk(chunk)
        )
      }
      nextOrDone <- httpClient.fetch(googleAuth.addAuth(gcsReq)) { response =>
        if (response.status.code == 308) {
          val bytesReceived = for {
            range <- response.headers.get(`Content-Range`)
            rangeEnd <- range.range.second
          } yield {
            rangeEnd
          }
          IO.pure(Left(bytesReceived.getOrElse(rangeStart + chunk.size)))
        } else if (response.status.isSuccess) {
          IO.pure(Right(()))
        } else {
          IO.raiseError(
            new RuntimeException(
              s"Failed to upload chunk to bucket gs://$bucket/$path, got status: ${response.status}"
            )
          )
        }
      }
    } yield {
      nextOrDone
    }
}

object AwsToGcpRunner {

  /** Build the REST API endpoint for a bucket/path in S3. */
  private def s3Uri(bucket: String, region: String, path: String): Uri =
    Uri.unsafeFromString(s"https://$bucket.s3-$region.amazonaws.com/$path")

  /** Build the REST API endpoint for a bucket/path in GCS. */
  private def baseGcsUri(bucket: String, path: String): Uri =
    Uri.unsafeFromString(s"https://www.googleapis.com/storage/v1/b/$bucket/o/$path")

  /** Build the REST API endpoint for a resumable upload to a GCS bucket. */
  private def baseGcsUploadUri(bucket: String): Uri =
    Uri
      .unsafeFromString(s"https://www.googleapis.com/upload/storage/v1/b/$bucket/o")
      .withQueryParam("uploadType", "resumable")

  private val bytesPerMib = 1048576

  /**
    * Number of bytes to pull from S3 / push to GCS at a time.
    *
    * Google recommends any file smaller than this be uploaded in a single request,
    * and Amazon doesn't allow multipart uploads with chunks smaller than this.
    */
  val ChunkSize: Int = 5 * bytesPerMib

  /** Construct an AWS -> GCP transfer runner, wrapped in setup/teardown logic for supporting clients. */
  def resource(config: RunnerConfig, ec: ExecutionContext)(
    implicit cs: ContextShift[IO],
    t: Timer[IO]
  ): Resource[IO, AwsToGcpRunner] =
    for {
      httpClient <- BlazeClientBuilder[IO](ec)
        .withResponseHeaderTimeout(config.timeouts.responseHeaderTimeout)
        .withRequestTimeout(config.timeouts.requestTimeout)
        .resource
      gcsAuth <- Resource.liftF(GcsAuthProvider.build(config.gcp))
    } yield {
      val s3Auth = new S3AuthProvider(config.aws)

      val retryPolicy = RetryPolicy[IO](
        RetryPolicy.exponentialBackoff(config.retries.maxDelay, config.retries.maxRetries)
      )
      val retryingClient = Retry(retryPolicy)(httpClient)

      new AwsToGcpRunner(
        Logger(logHeaders = true, logBody = false)(retryingClient),
        gcsAuth,
        s3Auth
      )
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

  /**
    * Error raised when initializing a GCS upload doesn't return an upload token.
    *
    * If you see this error, it means Google made a backwards-incompatible change to
    * their upload APIs.
    */
  case class NoToken(uri: String)
      extends IllegalStateException(
        s"Initiating resumable upload for $uri did not return an upload token"
      )
}
