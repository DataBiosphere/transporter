package org.broadinstitute.transporter.api

import io.circe.Decoder
import io.circe.derivation.deriveDecoder

case class SftpToGcsRequest(
  sftpPath: String,
  gcsBucket: String,
  gcsPath: String
)

object SftpToGcsRequest {
  implicit val decoder: Decoder[SftpToGcsRequest] = deriveDecoder
}
