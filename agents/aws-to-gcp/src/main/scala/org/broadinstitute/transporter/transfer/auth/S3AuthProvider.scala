package org.broadinstitute.transporter.transfer.auth

import java.net.URLEncoder
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import cats.effect.IO
import org.apache.commons.codec.digest.{DigestUtils, HmacUtils}
import org.broadinstitute.transporter.config.AwsConfig
import org.http4s.{Header, Headers, Method, Request, Uri}

class S3AuthProvider(config: AwsConfig) {
  import S3AuthProvider._

  def addAuth(s3Region: String, request: Request[IO]): IO[Request[IO]] =
    for {
      now <- IO.delay(LocalDateTime.now(ZoneId.of("UTC")))
      bodyBytes <- request.body.compile.toVector
    } yield {
      /*
       * First, add standard AWS headers.
       *
       * x-amz-content-sha256 is required for all requests using the v4 signature.
       * x-amz-date might be optional, but it's included in all the examples I can find.
       */
      val formattedNow = formatAmzDate(now)
      val hashedBody = DigestUtils.sha256Hex(bodyBytes.toArray)
      val requestWithAmzHeaders = request.transformHeaders(
        _.put(
          Header("x-amz-date", formattedNow),
          Header("x-amz-content-sha256", hashedBody)
        )
      )

      /*
       * Generate a canonical form of the request to use in the signature.
       *
       * The list of signed headers is included in both the canonical request and in the
       * eventual Authorization header, so it also pops out as a stand-alone output.
       */
      val (canonicalRequest, signedHeaders) = canonicalize(
        requestWithAmzHeaders.method,
        requestWithAmzHeaders.uri,
        requestWithAmzHeaders.headers,
        hashedBody
      )

      /*
       * Sign the canonical request.
       *
       * The signature is only valid within a specific AWS region/service scope.
       */
      val components = credScopeComponents(s3Region, now)
      val credScope = components.mkString("/")
      val stringToSign = buildStringToSign(canonicalRequest, formattedNow, credScope)
      val signature = sign(
        config.secretAccessKey,
        components,
        stringToSign
      )

      /*
       * Build & attach the Authorization header using the computed signature
       * and related info.
       */
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

  private[auth] def canonicalize(
    method: Method,
    uri: Uri,
    headers: Headers,
    hashedPayload: String
  ): (String, String) = {
    val canonicalUri = uri.query.params.toList
      .sortBy(_._1)
      .map {
        case (k, v) => s"$k=${URLEncoder.encode(v, "UTF-8")}"
      }
      .mkString("&")

    val sortedHeaders = headers.toList.map { h =>
      h.name.value.toLowerCase -> h.value
    }.sortBy(_._1)

    val signedHeaderNames = sortedHeaders.map(_._1).mkString(";")

    val canonicalRequest = List(
      method.name,
      uri.path,
      canonicalUri,
      sortedHeaders.map { case (k, v) => s"$k:$v\n" }.mkString,
      signedHeaderNames,
      hashedPayload.mkString
    ).mkString("\n")

    (canonicalRequest, signedHeaderNames)
  }

  private[this] val amzDateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")

  private def formatAmzDate(now: LocalDateTime): String =
    now.format(amzDateFormatter)

  private def credScopeComponents(
    region: String,
    now: LocalDateTime
  ): List[String] = List(
    now.format(DateTimeFormatter.BASIC_ISO_DATE),
    region,
    "s3",
    "aws4_request"
  )

  private[auth] def buildStringToSign(
    canonicalRequest: String,
    amzFormattedNow: String,
    credentialScope: String
  ): String =
    List(
      AWSAuthAlgorithm,
      amzFormattedNow,
      credentialScope,
      DigestUtils.sha256Hex(canonicalRequest.getBytes)
    ).mkString("\n")

  private[auth] def sign(
    secretKey: String,
    scopeComponents: List[String],
    stringToSign: String
  ): String = {
    val key = scopeComponents.foldLeft(s"AWS4$secretKey".getBytes) {
      (lastKey, nextComponent) =>
        HmacUtils.hmacSha256(lastKey, nextComponent.getBytes)
    }
    HmacUtils.hmacSha256Hex(key, stringToSign.getBytes)
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

}
