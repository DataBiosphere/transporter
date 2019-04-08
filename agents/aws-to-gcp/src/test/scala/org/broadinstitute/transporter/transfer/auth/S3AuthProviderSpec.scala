package org.broadinstitute.transporter.transfer.auth

import java.time.LocalDateTime

import org.http4s.{Headers, Method, Uri}
import org.http4s.headers._
import org.http4s.util.CaseInsensitiveString
import org.scalatest.{FlatSpec, Matchers, OptionValues}

class S3AuthProviderSpec extends FlatSpec with Matchers with OptionValues {
  behavior of "S3AuthProvider"

  // https://docs.aws.amazon.com/general/latest/gr/signature-v4-test-suite.html

  private val fakeAccessKey = "AKIDEXAMPLE"
  private val fakeSecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
  private val fakeRegion = "us-east-1"
  private val fakeService = "service"

  private val emptyStringHash =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

  private val now = LocalDateTime.of(2015, 8, 30, 12, 36, 0)

  private val testScope = "20150830/us-east-1/service/aws4_request"

  private val testHeaders = "host;x-amz-date"
  private val testCanonical =
    s"""GET
       |/
       |Param1=value1&Param2=value2
       |host:example.amazonaws.com
       |x-amz-date:20150830T123600Z
       |
       |$testHeaders
       |$emptyStringHash""".stripMargin

  private val hashedCanonical =
    "816cd5b414d056048ba4f7c5386d6e0533120fb1fcfa93762cf0fc39e2cf19e0"

  private val testSigningString =
    s"""AWS4-HMAC-SHA256
        |20150830T123600Z
        |20150830/us-east-1/service/aws4_request
        |$hashedCanonical""".stripMargin

  private val testSignature =
    "b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500"

  private val testRenderedAuth =
    s"""Authorization: AWS4-HMAC-SHA256 Credential="$fakeAccessKey/20150830/$fakeRegion/$fakeService/aws4_request",SignedHeaders="$testHeaders",Signature="$testSignature""""

  private val testHeaderSha = s"x-amz-content-sha256: $emptyStringHash"

  it should "hash payloads" in {
    S3AuthProvider
      .sha256("".getBytes)
      .map(S3AuthProvider.base16)
      .unsafeRunSync()
      .mkString shouldBe emptyStringHash
  }

  it should "create canonical requests" in {

    val (canonicalized, headerNames) = S3AuthProvider.canonicalize(
      method = Method.GET,
      uri = Uri.uri("/?Param2=value2&Param1=value1"),
      headers = Headers(Host("example.amazonaws.com")),
      hashedPayload = emptyStringHash,
      now = now
    )

    canonicalized shouldBe testCanonical
    headerNames shouldBe testHeaders
  }

  it should "create credential scopes" in {
    S3AuthProvider.credentialScope(fakeRegion, fakeService, now) shouldBe testScope
  }

  it should "build strings to sign" in {
    S3AuthProvider
      .sha256(testCanonical.getBytes)
      .map(S3AuthProvider.buildStringToSign(_, now, testScope))
      .unsafeRunSync() shouldBe testSigningString
  }

  it should "sign strings" in {
    S3AuthProvider
      .sign(fakeSecretKey, fakeRegion, fakeService, testSigningString, now)
      .unsafeRunSync() shouldBe testSignature
  }

  it should "build authorization headers" in {
    val headers = S3AuthProvider.buildHeaders(
      fakeAccessKey,
      testScope,
      testHeaders,
      testSignature,
      emptyStringHash
    )

    headers.get(Authorization).value.renderString shouldBe testRenderedAuth
    headers
      .get(CaseInsensitiveString("x-amz-content-sha256"))
      .value
      .renderString shouldBe testHeaderSha
  }
}
