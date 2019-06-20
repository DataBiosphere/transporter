package org.broadinstitute.transporter.transfer

import org.http4s._
import org.scalatest.{FlatSpec, Matchers}

class AwsToGcpRunnerSpec extends FlatSpec with Matchers {

  private val bucket = "test-bucket"

  behavior of "AwsToGcpRunner"

  it should "not include the default region in URI subdomains" in {
    val path = "foo/bar"

    AwsToGcpRunner.s3Uri(bucket, AwsToGcpRunner.DefaultAwsRegion, path) shouldBe
      Uri.unsafeFromString(s"https://$bucket.s3.amazonaws.com/$path")

    AwsToGcpRunner.s3Uri(bucket, "us-west-2", path) shouldBe
      Uri.unsafeFromString(s"https://$bucket.s3-us-west-2.amazonaws.com/$path")
  }

  it should "URL encode segments in S3 paths" in {
    val path = "path/to/**thing1**/special=true/[thing2]"

    AwsToGcpRunner.s3Uri(bucket, AwsToGcpRunner.DefaultAwsRegion, path) shouldBe
      Uri.unsafeFromString(
        s"https://$bucket.s3.amazonaws.com/path/to/%2A%2Athing1%2A%2A/special%3Dtrue/%5Bthing2%5D"
      )
  }
}
