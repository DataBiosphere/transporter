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

      credScope = credentialScope(config.region, S3Service, now)

      stringToSign = List(
        AWSAuthAlgorithm,
        now.format(DateFormatter),
        credScope,
        base16(hashedCanonicalRequest).mkString
      ).mkString("\n")

      signature <- sign(
        config.secretAccessKey,
        config.region,
        S3Service,
        stringToSign,
        now
      )
    } yield {
      request.transformHeaders(
        _ ++ buildHeaders(
          config.accessKeyId,
          credScope,
          signedHeaders,
          signature,
          hashedBody
        )
      )
    }

}

object S3AuthProvider {

  val AWSAuthAlgorithm = "AWS4-HMAC-SHA256"

  val S3Service = "s3"

  private val DateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")

  private[auth] def canonicalize(
    method: Method,
    uri: Uri,
    headers: Headers,
    hashedPayload: String,
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

  private[auth] def credentialScope(
    region: String,
    service: String,
    now: LocalDateTime
  ): String =
    s"${now.format(DateTimeFormatter.BASIC_ISO_DATE)}/$region/$service/aws4_request"

  private[auth] def buildStringToSign(
    hashedCanonicalRequest: Array[Byte],
    now: LocalDateTime,
    credentialScope: String
  ): String =
    List(
      AWSAuthAlgorithm,
      now.format(DateFormatter),
      credentialScope,
      base16(hashedCanonicalRequest)
    ).mkString("\n")

  private[auth] def sign(
    secretKey: String,
    region: String,
    service: String,
    canonicalRequest: String,
    now: LocalDateTime
  ): IO[String] = {
    val kSecret = s"AWS4$secretKey".getBytes

    for {
      kDate <- hmacSha256(now.format(DateTimeFormatter.BASIC_ISO_DATE).getBytes, kSecret)
      kRegion <- hmacSha256(region.getBytes, kDate)
      kService <- hmacSha256(service.getBytes, kRegion)
      key <- hmacSha256("aws4_request".getBytes, kService)
      signedRequest <- hmacSha256(canonicalRequest.getBytes, key)
    } yield {
      base16(signedRequest)
    }
  }

  private[auth] def buildHeaders(
    accessKey: String,
    credentialScope: String,
    signedHeaders: String,
    signature: String,
    hashedBody: String
  ): Headers = {
    val authCreds = Credentials.AuthParams(
      CaseInsensitiveString(AWSAuthAlgorithm),
      "Credential" -> s"$accessKey/$credentialScope",
      "SignedHeaders" -> signedHeaders,
      "Signature" -> signature
    )

    Headers(
      Authorization(authCreds),
      Header("x-amz-content-sha256", hashedBody)
    )
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

  private[this] val Base16 =
    Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')

  private[auth] def base16(data: Array[Byte]): String =
    data.flatMap(byte => Array(Base16(byte >> 4 & 0xF), Base16(byte & 0xF))).mkString

  private[this] def getDigest: IO[MessageDigest] =
    IO.delay(MessageDigest.getInstance("SHA-256"))

  private[auth] def sha256(data: Array[Byte]): IO[Array[Byte]] =
    getDigest.map(_.digest(data))

  private[auth] def sha256(data: Stream[IO, Byte]): IO[Array[Byte]] =
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
