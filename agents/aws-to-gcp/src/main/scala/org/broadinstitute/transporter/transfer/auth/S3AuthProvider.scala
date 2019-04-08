package org.broadinstitute.transporter.transfer.auth

import java.net.URLEncoder
import java.security.MessageDigest
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import cats.effect.IO
import fs2.Stream
import org.broadinstitute.transporter.config.AwsConfig
import org.http4s.headers.Authorization
import org.http4s.util.CaseInsensitiveString
import org.http4s.{Credentials, Header, Headers, Method, Query, Request, Uri}

class S3AuthProvider(config: AwsConfig) {
  import S3AuthProvider._

  def addAuth(request: Request[IO]): IO[Request[IO]] =
    for {
      now <- IO.delay(LocalDateTime.now())
      hashedBody <- sha256(request.body).map(base16)

      (canonicalRequest, signedHeaders) = canonicalize(
        request.method,
        request.uri,
        request.headers,
        hashedBody,
        now
      )

      hashedCanonicalRequest <- sha256(canonicalRequest.getBytes)

      credentialScope = s"${now.format(DateTimeFormatter.BASIC_ISO_DATE)}/${config.region}/s3/aws4_request"

      stringToSign = List(
        AWSAuthAlgorithm,
        now.format(DateFormatter),
        credentialScope,
        base16(hashedCanonicalRequest).mkString
      ).mkString("\n")

      signature <- sign(stringToSign, now)
    } yield {

      val authCreds = Credentials.AuthParams(
        CaseInsensitiveString(AWSAuthAlgorithm),
        "Credential" -> s"${config.accessKeyId}/$credentialScope",
        "SignedHeaders" -> signedHeaders,
        "Signature" -> signature.mkString
      )

      val newHeaders = Headers(
        Authorization(authCreds),
        Header("x-amz-content-sha256", hashedBody.mkString)
      )

      request.transformHeaders(_ ++ newHeaders)
    }

  private def sign(canonicalRequest: String, now: LocalDateTime): IO[Array[Byte]] = {
    val kSecret = s"AWS4${config.secretAccessKey}".getBytes

    for {
      kDate <- hmacSha256(now.format(DateTimeFormatter.BASIC_ISO_DATE).getBytes, kSecret)
      kRegion <- hmacSha256(config.region.getBytes, kDate)
      kService <- hmacSha256("s3".getBytes, kRegion)
      key <- hmacSha256("aws4_request".getBytes, kService)
      signedRequest <- hmacSha256(canonicalRequest.getBytes, key)
    } yield {
      base16(signedRequest)
    }
  }
}

object S3AuthProvider {

  val AWSAuthAlgorithm = "AWS-HMAC-SHA256"

  private val DateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")

  private def canonicalize(
    method: Method,
    uri: Uri,
    headers: Headers,
    hashedPayload: Array[Byte],
    now: LocalDateTime
  ): (String, String) = {
    val allHeaders = headers.put(
      Header("x-amz-date", now.format(DateFormatter))
    )

    val signedHeaderNames =
      allHeaders.toList.map(_.name.value.toLowerCase).sorted.mkString(";")

    val canonicalRequest = List(
      method.name,
      uri.path,
      canonicalize(uri.query),
      canonicalize(allHeaders),
      signedHeaderNames,
      hashedPayload.mkString
    ).mkString("\n")

    (canonicalRequest, signedHeaderNames)
  }

  private[this] def canonicalize(query: Query): String =
    query.params.toList
      .sortBy(_._1)
      .map {
        case (k, v) => s"$k=${URLEncoder.encode(v, "UTF-8")}"
      }
      .mkString("&")

  private[this] def canonicalize(headers: Headers): String =
    headers.toList
      .sortBy(_.name.value.toLowerCase)
      .map(h => s"${h.name.value.toLowerCase}:${h.value}\n")
      .mkString

  private[this] val Base16: Array[Byte] =
    Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')

  private def base16(data: Array[Byte]): Array[Byte] =
    data.flatMap(byte => Array(Base16(byte >> 4 & 0xF), Base16(byte & 0xF)))

  private[this] def getDigest: IO[MessageDigest] =
    IO.delay(MessageDigest.getInstance("SHA-256"))

  private def sha256(data: Array[Byte]): IO[Array[Byte]] = {
    getDigest.map { digest =>
      digest.update(data)
      digest.digest()
    }
  }

  private def sha256(data: Stream[IO, Byte]): IO[Array[Byte]] =
    for {
      digest <- getDigest
      updated <- data.chunks.compile.fold(digest) { (d, chunk) =>
        d.update(chunk.toByteBuffer)
        d
      }
    } yield {
      updated.digest()
    }

  private def hmacSha256(data: Array[Byte], key: Array[Byte]): IO[Array[Byte]] = {
    IO.delay(Mac.getInstance("HmacSHA256")).map { mac =>
      mac.init(new SecretKeySpec(key, "HmacSHA256"))
      mac.doFinal(data)
    }
  }
}
