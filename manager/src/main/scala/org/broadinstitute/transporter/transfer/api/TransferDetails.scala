package org.broadinstitute.transporter.transfer.api

import java.time.OffsetDateTime
import java.util.UUID

import io.circe.{Encoder, Json}
import io.circe.derivation.deriveEncoder
import org.broadinstitute.transporter.transfer.TransferStatus

case class TransferDetails(
  id: UUID,
  status: TransferStatus,
  requestBody: Json,
  submittedAt: Option[OffsetDateTime],
  updatedAt: Option[OffsetDateTime],
  reportedInfo: Option[Json]
)

object TransferDetails {
  implicit val encoder: Encoder[TransferDetails] = deriveEncoder
}
