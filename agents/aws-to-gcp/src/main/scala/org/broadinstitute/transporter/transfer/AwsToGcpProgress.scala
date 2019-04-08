package org.broadinstitute.transporter.transfer

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

case class AwsToGcpProgress(
  s3Bucket: String,
  s3Path: String,
  gcsBucket: String,
  gcsPath: String,
  gcsToken: String,
  bytesUploaded: Long,
  totalBytes: Long
)

object AwsToGcpProgress {
  implicit val decoder: Decoder[AwsToGcpProgress] = deriveDecoder
  implicit val encoder: Encoder[AwsToGcpProgress] = deriveEncoder
}
