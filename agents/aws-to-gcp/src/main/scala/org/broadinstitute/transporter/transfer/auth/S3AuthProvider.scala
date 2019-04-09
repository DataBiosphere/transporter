package org.broadinstitute.transporter.transfer.auth

import java.net.URLEncoder
import java.security.MessageDigest
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import cats.effect.IO
import fs2.Stream
import org.broadinstitute.transporter.config.AwsConfig
import org.http4s.{Header, Headers, Method, Query, Request, Uri}

class S3AuthProvider(config: AwsConfig) {
  import S3AuthProvider._

  def addAuth(s3Region: String, request: Request[IO]): IO[Request[IO]] =
    for {
      now <- IO.delay(LocalDateTime.now(ZoneId.of("UTC")))
      hashedBody <- sha256(request.body).map(base16)

      requestWithAmzHeaders = request.transformHeaders(
        _.put(
          Header("x-amz-date", now.format(DateFormatter)),
          Header("x-amz-content-sha256", hashedBody)
        )
      )

      (canonicalRequest, signedHeaders) = canonicalize(
        requestWithAmzHeaders.method,
        requestWithAmzHeaders.uri,
        requestWithAmzHeaders.headers,
        hashedBody
      )

      credScope = credentialScope(s3Region, S3Service, now)
      stringToSign <- buildStringToSign(canonicalRequest, now, credScope)

      signature <- sign(
        config.secretAccessKey,
        s3Region,
        S3Service,
        stringToSign,
        now
      )
    } yield {
      val auth = authHeader(
        config.accessKeyId,
        credScope,
        signedHeaders,
        signature
      )

      requestWithAmzHeaders.transformHeaders(_.put(auth))
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
    hashedPayload: String
  ): (String, String) = {
    val sortedHeaders = canonicalSort(headers)
    val signedHeaderNames = sortedHeaders.map(_._1).mkString(";")

    val canonicalRequest = List(
      method.name,
      uri.path,
      canonicalize(uri.query),
      canonicalize(sortedHeaders),
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
    canonicalRequest: String,
    now: LocalDateTime,
    credentialScope: String
  ): IO[String] =
    sha256(canonicalRequest.getBytes).map { hashedRequest =>
      List(
        AWSAuthAlgorithm,
        now.format(DateFormatter),
        credentialScope,
        base16(hashedRequest)
      ).mkString("\n")
    }

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

  private[auth] def authHeader(
    accessKey: String,
    credentialScope: String,
    signedHeaders: String,
    signature: String
  ): Header = Header(
    "Authorization",
    s"$AWSAuthAlgorithm Credential=$accessKey/$credentialScope,SignedHeaders=$signedHeaders,Signature=$signature"
  )

  private[this] def canonicalize(query: Query): String =
    query.params.toList
      .sortBy(_._1)
      .map {
        case (k, v) => s"$k=${URLEncoder.encode(v, "UTF-8")}"
      }
      .mkString("&")

  private[this] def canonicalSort(headers: Headers): List[(String, String)] =
    headers.toList.map { h =>
      h.name.value.toLowerCase -> h.value
    }.sortBy(_._1)

  private[this] def canonicalize(sortedHeaders: List[(String, String)]): String =
    sortedHeaders.map { case (k, v) => s"$k:$v\n" }.mkString

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
