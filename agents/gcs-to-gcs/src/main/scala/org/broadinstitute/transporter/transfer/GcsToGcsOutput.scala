package org.broadinstitute.transporter.transfer

import io.circe.Encoder
import io.circe.derivation.deriveEncoder

case class GcsToGcsOutput(gcsBucket: String, gcsPath: String)

object GcsToGcsOutput {
  implicit val encoder: Encoder[GcsToGcsOutput] = deriveEncoder
}
