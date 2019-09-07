package org.broadinstitute.transporter.transfer

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

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
