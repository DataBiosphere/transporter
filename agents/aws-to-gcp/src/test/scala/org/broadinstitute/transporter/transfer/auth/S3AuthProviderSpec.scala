package org.broadinstitute.transporter.transfer.auth

import org.http4s.{Header, Headers, Method, Uri}
import org.http4s.headers._
import org.scalatest.{FlatSpec, Matchers}

class S3AuthProviderSpec extends FlatSpec with Matchers {
  behavior of "S3AuthProvider"

  // https://docs.aws.amazon.com/general/latest/gr/signature-v4-test-suite.html

  private val fakeAccessKey = "AKIDEXAMPLE"
  private val fakeSecretKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
  private val fakeRegion = "us-east-1"
  private val fakeService = "service"

  private val emptyStringHash =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

  private val nowDate = "20150830"
  private val nowAmz = s"${nowDate}T123600Z"

  private val testScope = s"$nowDate/$fakeRegion/$fakeService/aws4_request"

  private val testHeaders = "host;x-amz-date"
  private val testCanonical =
    s"""GET
       |/
       |Param1=value1&Param2=value2
       |host:example.amazonaws.com
       |x-amz-date:$nowAmz
       |
       |$testHeaders
       |$emptyStringHash""".stripMargin

  private val hashedCanonical =
    "816cd5b414d056048ba4f7c5386d6e0533120fb1fcfa93762cf0fc39e2cf19e0"

  private val testSigningString =
    s"""AWS4-HMAC-SHA256
       |$nowAmz
       |$testScope
       |$hashedCanonical""".stripMargin

  private val testSignature =
    "b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500"

  private val testRenderedAuth =
    s"""Authorization: AWS4-HMAC-SHA256 Credential=$fakeAccessKey/$testScope,SignedHeaders=$testHeaders,Signature=$testSignature"""

  it should "create canonical requests" in {

    val (canonicalized, headerNames) = S3AuthProvider.canonicalize(
      method = Method.GET,
      uri = Uri.uri("https://bucket.s3.amazonaws.com/?Param2=value2&Param1=value1"),
      headers = Headers.of(Host("example.amazonaws.com"), Header("x-amz-date", nowAmz)),
      hashedPayload = emptyStringHash
    )

    canonicalized shouldBe testCanonical
    headerNames shouldBe testHeaders
  }

  it should "insert a blank placeholder into canonical requests without query strings" in {
    val (canonicalized, _) = S3AuthProvider.canonicalize(
      method = Method.PUT,
      uri = Uri.uri("https://bucket.s3.amazonaws.com/thing1/thing2.file"),
      headers = Headers.of(Header("x-amz-date", nowAmz)),
      hashedPayload = emptyStringHash
    )

    canonicalized shouldBe
      s"""PUT
         |/thing1/thing2.file
         |
         |x-amz-date:20150830T123600Z
         |
         |x-amz-date
         |$emptyStringHash""".stripMargin
  }

  it should "build strings to sign" in {
    S3AuthProvider.buildStringToSign(testCanonical, nowAmz, testScope) shouldBe testSigningString
  }

  it should "sign strings" in {
    val components = List(nowDate, fakeRegion, fakeService, "aws4_request")
    S3AuthProvider.sign(fakeSecretKey, components, testSigningString) shouldBe testSignature
  }

  it should "build authorization headers" in {
    val auth = S3AuthProvider.authHeader(
      fakeAccessKey,
      testScope,
      testHeaders,
      testSignature
    )

    auth.renderString shouldBe testRenderedAuth
  }
}
