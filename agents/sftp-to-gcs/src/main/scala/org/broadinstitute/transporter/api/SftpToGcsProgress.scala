package org.broadinstitute.transporter.api

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Incremental progress marker for an SFTP->GCS transfer.
  *
  * @param sftpPath path within the configured SFTP site (without leading /) pointing at the file being copied
  * @param gcsBucket name of the GCS bucket (without leading gs://) the file is being copied into
  * @param gcsPath path within `gcsBucket` (without leading /) the file is being copied into
  * @param gcsToken API token for the ongoing GCS resumable upload
  * @param bytesUploaded number of bytes uploaded into GCS so far
  * @param totalBytes total number of bytes to upload into GCS
  */
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
