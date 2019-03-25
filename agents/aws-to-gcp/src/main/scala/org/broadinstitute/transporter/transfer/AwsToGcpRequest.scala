package org.broadinstitute.transporter.transfer

import io.circe.Decoder
import io.circe.derivation.deriveDecoder

case class AwsToGcpRequest(
  s3Bucket: String,
  s3Path: String,
  gcsBucket: String,
  gcsPath: String,
  expectedSize: Option[Long],
  expectedMd5: Option[String]
)

object AwsToGcpRequest {
  implicit val decoder: Decoder[AwsToGcpRequest] = deriveDecoder
}
