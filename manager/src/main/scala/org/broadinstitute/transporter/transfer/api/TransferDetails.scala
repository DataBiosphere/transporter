package org.broadinstitute.transporter.transfer.api

import java.time.OffsetDateTime
import java.util.UUID

import io.circe.{Encoder, Json}
import io.circe.derivation.deriveEncoder
import org.broadinstitute.transporter.transfer.TransferStatus

/**
  * Detailed information about a transfer job held by the manager.
  *
  * @param id unique ID of the transfer within its enclosing request
  * @param status current status of the transfer
  * @param requestBody user-provided JSON payload describing the transfer
  * @param submittedAt time at which the transfer was pushed to agents
  * @param updatedAt time at which the latest status update was received for the transfer
  * @param reportedInfo JSON output sent to the manager by an agent about the transfer
  */
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
