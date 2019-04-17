package org.broadinstitute.transporter.transfer

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Incremental progress marker for an AWS -> GCP transfer.
  *
  * @param s3Bucket name of the S3 bucket (without leading s3://) containing the file being copied
  * @param s3Region region hosting `s3Bucket`
  * @param s3Path path within `s3Bucket` (without leading /) pointing to the file being copied
  * @param gcsBucket name of the GCS bucket (without leading gs://) the file is being copied into
  * @param gcsPath path within `gcsBucket` (without leading /) the file is being copied to
  * @param gcsToken resumable upload token assigned by
  * @param bytesUploaded number of bytes already copied from `s3Path` to `gcsPath`
  * @param totalBytes total number of bytes to copy from `s3Path` to `gcsPath`
  */
case class AwsToGcpProgress(
  s3Bucket: String,
  s3Region: String,
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
