package org.broadinstitute.transporter.transfer.auth

import java.net.URLEncoder
import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

import cats.effect.IO
import org.apache.commons.codec.digest.{DigestUtils, HmacAlgorithms, HmacUtils}
import org.broadinstitute.transporter.config.AwsConfig
import org.http4s.{Header, Headers, Method, Request, Uri}

/**
  * Utility which can add authorization info for S3 to outgoing HTTP requests.
  *
  * The AWS SDK tightly encapsulates the logic for generating auth info, so we
  * reimplement the algorithm here.
  *
  * @see https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html
  */
class S3AuthProvider(config: AwsConfig) {
  import S3AuthProvider._

  /**
    * Add authorization for S3 within a specific region to an HTTP request.
    *
    * Unlike Google, AWS ties its security keys to specific regions. We could
    * potentially try to derive the correct region for each request by parsing
    * error responses, but it's easier to ask for the region up-front (and that's
    * what the AWS team recommends).
    */
  def addAuth(s3Region: String, request: Request[IO]): IO[Request[IO]] =
    for {
      now <- IO.delay(Instant.now())
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
  private val AWSAuthAlgorithm = "AWS4-HMAC-SHA256"

  /**
    * Assemble the "canonical" form of an outgoing HTTP request for AWS signing.
    *
    * @see https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
    *
    * @return both the canonicalized request and the stringified list of canonical
    *         headers signed within the request
    */
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

  private[this] val amzDateFormatter =
    DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")

  /** Format an instant for use in the "x-amz-date" HTTP header. */
  private def formatAmzDate(now: Instant): String =
    now.atOffset(ZoneOffset.UTC).format(amzDateFormatter)

  private[this] val scopeDateFormatter =
    DateTimeFormatter.ofPattern("yyyyMMdd")

  /** Build the tail components of the "credential scope" for AWS signing. */
  private def credScopeComponents(
    region: String,
    now: Instant
  ): List[String] = List(
    now.atOffset(ZoneOffset.UTC).format(scopeDateFormatter),
    region,
    "s3",
    "aws4_request"
  )

  /**
    * Combine request meta-information into the string that should be converted
    * into the request's signature.
    *
    * @see https://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html
    */
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

  /**
    * Convert request meta-information into an authorization signature.
    *
    * @see https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
    */
  private[auth] def sign(
    secretKey: String,
    scopeComponents: List[String],
    stringToSign: String
  ): String = {
    val key = scopeComponents.foldLeft(s"AWS4$secretKey".getBytes) {
      (lastKey, nextComponent) =>
        new HmacUtils(HmacAlgorithms.HMAC_SHA_256, lastKey).hmac(nextComponent)
    }
    new HmacUtils(HmacAlgorithms.HMAC_SHA_256, key).hmacHex(stringToSign)
  }

  /**
    * Build an HTTP Authorization header from computed AWS security info.
    *
    * @see https://docs.aws.amazon.com/general/latest/gr/sigv4-add-signature-to-request.html
    */
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
