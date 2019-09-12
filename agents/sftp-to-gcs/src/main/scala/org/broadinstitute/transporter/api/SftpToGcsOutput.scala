package org.broadinstitute.transporter.api

import io.circe.Encoder
import io.circe.derivation.deriveEncoder

/**
  * Output of a successful SFTP->GCS transfer.
  *
  * @param gcsBucket name of the GCS bucket (without leading gs://) the file was copied into
  * @param gcsPath path within `gcsBucket` (without leading /) the file was copied into
  */
case class SftpToGcsOutput(gcsBucket: String, gcsPath: String)

object SftpToGcsOutput {
  implicit val encoder: Encoder[SftpToGcsOutput] = deriveEncoder
}
