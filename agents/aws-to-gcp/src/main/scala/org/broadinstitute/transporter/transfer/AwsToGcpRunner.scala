package org.broadinstitute.transporter.transfer

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import fs2.{Chunk, Stream}
import io.circe.{Json, JsonObject}
import io.circe.syntax._
import org.apache.commons.codec.binary.{Base64, Hex}
import org.broadinstitute.transporter.config.RunnerConfig
import org.broadinstitute.transporter.transfer.auth.{GcsAuthProvider, S3AuthProvider}
import org.http4s._
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.RequestLogger
import org.http4s.headers._
import org.http4s.util.CaseInsensitiveString

import scala.concurrent.ExecutionContext

class AwsToGcpRunner(
  httpClient: Client[IO],
  googleAuth: GcsAuthProvider,
  s3Auth: S3AuthProvider
) extends TransferRunner {
  import AwsToGcpRunner._

  /*--- BOILERPLATE ---*/

  override type In = AwsToGcpRequest
  override type Progress = AwsToGcpProgress
  override type Out = AwsToGcpOutput

  override def decodeInput(json: Json): Either[Throwable, AwsToGcpRequest] =
    json.as[AwsToGcpRequest]

  override def decodeProgress(json: Json): Either[Throwable, AwsToGcpProgress] =
    json.as[AwsToGcpProgress]

  override def encodeProgress(progress: AwsToGcpProgress): Json = progress.asJson

  override def encodeOutput(output: AwsToGcpOutput): Json = output.asJson

  /*--- END BOILERPLATE ---*/

  override def initialize(request: AwsToGcpRequest): AwsToGcpProgress = {

    val initFlow = for {
      s3Metadata <- getS3Metadata(request.s3Bucket, request.s3Path, request.expectedSize)
      uploadToken <- initGcsResumableUpload(
        request.gcsBucket,
        request.gcsPath,
        s3Metadata,
        request.expectedMd5
      )
    } yield {
      AwsToGcpProgress(
        request.s3Bucket,
        request.s3Path,
        request.gcsBucket,
        request.gcsPath,
        uploadToken,
        0L,
        s3Metadata.contentLength
      )
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
    */
  private def getS3Metadata(
    bucket: String,
    path: String,
    expectedSize: Option[Long]
  ): IO[S3Metadata] = {
    val s3DebugUri = s"s3://$bucket/$path"

    for {
      s3Req <- s3Auth.addAuth {
        Request(
          method = Method.HEAD,
          uri = s3Uri(bucket, path)
        )
      }
      metadata <- httpClient.fetch(s3Req) { response =>
        response.contentLength.liftTo[IO](NoFileSize(s3DebugUri)).map {
          S3Metadata(_, response.contentType)
        }
      }
      _ <- expectedSize.filter(_ != metadata.contentLength).fold(IO.unit) {
        expectedSize =>
          IO.raiseError(
            UnexpectedFileSize(s3DebugUri, expectedSize, metadata.contentLength)
          )
      }
    } yield {
      metadata
    }
  }

  private def initGcsResumableUpload(
    bucket: String,
    path: String,
    s3Metadata: S3Metadata,
    expectedMd5: Option[String]
  ): IO[String] = {
    // Object metadata is used by Google to register the upload to the correct pseudo-path.
    val baseObjectMetadata = JsonObject("name" -> path.asJson)
    // Including the "md5Hash" metadata triggers server-side validation on upload.
    val objectMetadata = expectedMd5
      .fold(baseObjectMetadata) { hexMd5 =>
        baseObjectMetadata.add("md5Hash", hexToBase64(hexMd5).asJson)
      }
      .asJson

    val initBody = Stream.emits(objectMetadata.noSpaces.getBytes)

    for {
      initSize <- initBody.covary[IO].compile.fold(0L)((s, _) => s + 1)
      initHeaders = Headers(
        `Content-Length`.unsafeFromLong(initSize),
        `Content-Type`(MediaType.application.json, Charset.`UTF-8`),
        Header("X-Upload-Content-Length", s3Metadata.contentLength.toString)
      )
      gcsInitReq <- googleAuth.addAuth {
        Request(
          method = Method.POST,
          uri = baseGcsUploadUri(bucket),
          body = initBody,
          headers = s3Metadata.contentType.fold(initHeaders) { s3ContentType =>
            initHeaders.put(Header("X-Upload-Content-Type", s3ContentType.renderString))
          }
        )
      }
      locationHeader <- httpClient.fetch(gcsInitReq) { response =>
        response.headers
          .get(CaseInsensitiveString("Location"))
          .liftTo[IO](NoToken(s"gs://$bucket/$path"))
      }
    } yield {
      locationHeader.value
    }

  }

  private def hexToBase64(hex: String): String =
    Base64.encodeBase64String(Hex.decodeHex(hex.toCharArray))

  override def step(
    progress: AwsToGcpProgress
  ): Either[AwsToGcpProgress, AwsToGcpOutput] = {
    val doStep = for {
      chunk <- getS3Chunk(
        progress.s3Bucket,
        progress.s3Path,
        progress.bytesUploaded,
        progress.bytesUploaded + ChunkSize - 1
      )
      nextOrDone <- uploadChunk(
        progress.gcsBucket,
        progress.gcsPath,
        progress.gcsToken,
        progress.bytesUploaded,
        chunk
      )
    } yield {
      nextOrDone.bimap(
        bytesUploaded => progress.copy(bytesUploaded = bytesUploaded),
        _ => AwsToGcpOutput(progress.gcsBucket, progress.gcsPath)
      )
    }

    doStep.unsafeRunSync()
  }

  private def getS3Chunk(
    bucket: String,
    path: String,
    rangeStart: Long,
    rangeEnd: Long
  ): IO[Chunk[Byte]] =
    for {
      s3Req <- s3Auth.addAuth {
        Request(
          method = Method.HEAD,
          uri = s3Uri(bucket, path),
          headers = Headers(`Content-Range`(rangeStart, rangeEnd))
        )
      }
      chunk <- httpClient.expect[Chunk[Byte]](s3Req)
    } yield {
      chunk
    }

  private def uploadChunk(
    bucket: String,
    path: String,
    uploadToken: String,
    rangeStart: Long,
    chunk: Chunk[Byte]
  ): IO[Either[Long, Unit]] =
    for {
      gcsReq <- googleAuth.addAuth {
        Request(
          method = Method.PUT,
          uri = baseGcsUploadUri(bucket).withQueryParam("upload_id", uploadToken),
          headers = Headers(
            `Content-Length`.unsafeFromLong(chunk.size.toLong),
            `Content-Range`(rangeStart, rangeStart + chunk.size - 1)
          )
        )
      }
      nextOrDone <- httpClient.fetch(gcsReq) { response =>
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
            new RuntimeException(s"Failed to upload chunk to bucket gs://$bucket/$path")
          )
        }
      }
    } yield {
      nextOrDone
    }

  private def s3Uri(bucket: String, path: String): Uri =
    Uri.unsafeFromString(s"https://$bucket.s3.amazonaws.com/$path")

  private def baseGcsUploadUri(bucket: String): Uri =
    Uri
      .unsafeFromString(s"https://www.googleapis.com/upload/storage/v1/b/$bucket/o")
      .withQueryParam("uploadType", "resumable")
}

object AwsToGcpRunner {

  private val bytesPerMib = math.pow(2, 20).toInt

  /**
    * Number of bytes to pull from S3 / push to GCS at a time.
    *
    * Default chunk size for Google Cloud Java's write channels.
    */
  val ChunkSize: Int = 2 * bytesPerMib

  /** Max number of chunks to store in-memory between the parallel download & upload streams. */
  val BufferSize: Int = (512 * bytesPerMib) / ChunkSize

  def resource(config: RunnerConfig, ec: ExecutionContext)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, AwsToGcpRunner] =
    for {
      httpClient <- BlazeClientBuilder[IO](ec).resource
      gcsAuth <- Resource.liftF(GcsAuthProvider.build(config.gcp))
    } yield {
      val s3Auth = new S3AuthProvider(config.aws)
      new AwsToGcpRunner(
        RequestLogger(logHeaders = true, logBody = true)(httpClient),
        gcsAuth,
        s3Auth
      )
    }

  case class S3Metadata(contentLength: Long, contentType: Option[`Content-Type`])

  case class NoFileSize(uri: String)
      extends IllegalStateException(
        s"Object $uri doesn't have a recorded size"
      )

  /** Exception raised when the size of the source S3 object doesn't match a request's expectations. */
  case class UnexpectedFileSize(uri: String, expected: Long, actual: Long)
      extends IllegalStateException(
        s"Object $uri has size $actual, but expected $expected"
      )

  case class NoToken(uri: String)
      extends IllegalStateException(
        s"Initiating resumable upload for $uri did not return an upload token"
      )
}
