package org.broadinstitute.transporter.api

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

case class SftpToGcsProgress(
  sftpPath: String,
  gcsBucket: String,
  gcsPath: String,
  gcsToken: String,
  bytesUploaded: Long,
  totalBytes: Long
)

object SftpToGcsProgress {
  implicit val decoder: Decoder[SftpToGcsProgress] = deriveDecoder
  implicit val encoder: Encoder[SftpToGcsProgress] = deriveEncoder
}
