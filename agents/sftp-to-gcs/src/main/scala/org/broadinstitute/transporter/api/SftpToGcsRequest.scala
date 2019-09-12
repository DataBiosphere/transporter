package org.broadinstitute.transporter.api

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * A request to copy a single file from an SFTP site to a GCS bucket.
  *
  * @param sftpPath path within the configured SFTP site (without leading /) pointing at the file-to-copy
  * @param gcsBucket name of the GCS bucket (without leading gs://) to copy the file into
  * @param gcsPath path within `gcsBucket` (without leading /) to copy the file into
  * @param isDirectory if `true`, `sftpPath` is treated as a directory which should be recursively copied
  *                    into the prefix `gcsPath`
  */
case class SftpToGcsRequest(
  sftpPath: String,
  gcsBucket: String,
  gcsPath: String,
  isDirectory: Boolean
)

object SftpToGcsRequest {
  implicit val decoder: Decoder[SftpToGcsRequest] = deriveDecoder
  implicit val encoder: Encoder[SftpToGcsRequest] = deriveEncoder
}
