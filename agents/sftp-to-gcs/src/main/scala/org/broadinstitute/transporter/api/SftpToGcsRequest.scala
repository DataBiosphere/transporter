package org.broadinstitute.transporter.api

import io.circe.Decoder
import io.circe.derivation.deriveDecoder

/**
  * A request to copy a single file from an SFTP site to a GCS bucket.
  *
  * @param sftpPath path within the configured SFTP site (without leading /) pointing at the file-to-copy
  * @param gcsBucket name of the GCS bucket (without leading gs://) to copy the file into
  * @param gcsPath path within `gcsBucket` (without leading /) to copy the file into
  */
case class SftpToGcsRequest(
  sftpPath: String,
  gcsBucket: String,
  gcsPath: String
)

object SftpToGcsRequest {
  implicit val decoder: Decoder[SftpToGcsRequest] = deriveDecoder
}
