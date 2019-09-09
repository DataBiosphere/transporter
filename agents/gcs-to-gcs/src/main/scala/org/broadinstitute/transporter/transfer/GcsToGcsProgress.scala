package org.broadinstitute.transporter.transfer

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Incremental progress marker for a GCS-internal transfer.
  *
  * @param sourceBucket name of the GCS bucket (without leading gs://) containing the file being copied
  * @param sourcePath path within `sourceBucket` (without leading /) pointing to the file being copied
  * @param targetBucket name of teh GCS bucket (without leading gs://) the file is being copied into
  * @param targetPath path within `targetBucket` the file is being copied into
  * @param uploadId latest "rewrite token" received by the GCS copy API
  */
case class GcsToGcsProgress(
  sourceBucket: String,
  sourcePath: String,
  targetBucket: String,
  targetPath: String,
  uploadId: String
)

object GcsToGcsProgress {
  implicit val decoder: Decoder[GcsToGcsProgress] = deriveDecoder
  implicit val encoder: Encoder[GcsToGcsProgress] = deriveEncoder
}
