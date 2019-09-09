package org.broadinstitute.transporter.transfer

import io.circe.Encoder
import io.circe.derivation.deriveEncoder

/**
  * Output of a successful GCS-internal transfer.
  *
  * @param gcsBucket name of the GCS bucket (without leading gs://) the file was copied into
  * @param gcsPath path within `gcsBucket` (without leading /) the file was copied into
  */
case class GcsToGcsOutput(gcsBucket: String, gcsPath: String)

object GcsToGcsOutput {
  implicit val encoder: Encoder[GcsToGcsOutput] = deriveEncoder
}
