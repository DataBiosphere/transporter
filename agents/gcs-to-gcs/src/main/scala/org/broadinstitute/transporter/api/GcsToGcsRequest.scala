package org.broadinstitute.transporter.api

import io.circe.Decoder
import io.circe.derivation.deriveDecoder

/**
  * A request to copy a single file from one location in GCS to another.
  *
  * @param sourceBucket name of the GCS bucket (without leading gs://) containing the file-to-copy
  * @param sourcePath path within `sourceBucket` (without leading /) pointing to the file-to-copy
  * @param targetBucket name of the GCS bucket (without leading gs://) to copy the file into
  * @param targetPath path within `targetBucket` (without leading /) to copy the file into
  */
case class GcsToGcsRequest(
  sourceBucket: String,
  sourcePath: String,
  targetBucket: String,
  targetPath: String
)

object GcsToGcsRequest {
  implicit val decoder: Decoder[GcsToGcsRequest] = deriveDecoder
}
