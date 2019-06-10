package org.broadinstitute.transporter.transfer

import io.circe.Decoder
import io.circe.derivation.deriveDecoder

/**
  * A request to copy a single file from AWS to GCP.
  *
  * @param s3Bucket name of the S3 bucket (without leading s3://) containing the file-to-copy
  * @param s3Region region hosting `s3Bucket`. S3 doesn't have a concept of multi-regional buckets
  *                 like Google does, and it requires regions to be included in request signatures
  *                 when making authorized requests
  * @param s3Path path within `s3Bucket` (without leading /) pointing to the file-to-copy
  * @param gcsBucket name of the GCS bucket (without leading gs://) to copy the file into
  * @param gcsPath path within `gcsBucket` (without leading /) to copy the file into
  * @param force if true, overwrite any existing file at 'gs://gcsBucket/gcsPath'; otherwise fail
  *              the transfer if an object already exists at that path with a different MD5 than is expected
  * @param expectedSize expected Content-Length value of 's3://s3Bucket/s3Path'; the transfer
  *                     will bail out early if a different size is found
  * @param expectedMd5 expected content md5 of 's3://s3bucket/s3Path'; the transfer will fail
  *                    to complete if a different md5 is computed
  */
case class AwsToGcpRequest(
  s3Bucket: String,
  s3Region: String,
  s3Path: String,
  gcsBucket: String,
  gcsPath: String,
  force: Option[Boolean],
  expectedSize: Option[Long],
  expectedMd5: Option[String]
)

object AwsToGcpRequest {
  implicit val decoder: Decoder[AwsToGcpRequest] = deriveDecoder
}
