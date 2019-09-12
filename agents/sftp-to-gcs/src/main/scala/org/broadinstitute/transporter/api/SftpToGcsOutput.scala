package org.broadinstitute.transporter.api

import io.circe.Encoder
import io.circe.derivation.deriveEncoder

case class SftpToGcsOutput(gcsBucket: String, gcsPath: String)

object SftpToGcsOutput {
  implicit val encoder: Encoder[SftpToGcsOutput] = deriveEncoder
}
