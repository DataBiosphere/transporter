package org.broadinstitute.transporter.transfer

import io.circe.Decoder
import io.circe.derivation.deriveDecoder

case class GcsToGcsRequest(
  sourceBucket: String,
  sourcePath: String,
  targetBucket: String,
  targetPath: String
)

object GcsToGcsRequest {
  implicit val decoder: Decoder[GcsToGcsRequest] = deriveDecoder
}
