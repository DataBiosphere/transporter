package org.broadinstitute.transporter.transfer.api

import java.time.OffsetDateTime
import java.util.UUID

import io.circe.{Decoder, Encoder, Json}
import io.circe.derivation.{deriveDecoder, deriveEncoder}
import org.broadinstitute.transporter.transfer.TransferStatus

/**
  * Detailed information about a transfer job held by the manager.
  *
  * @param id unique ID of the transfer within its enclosing request
  * @param status current status of the transfer
  * @param priority the submission priority of the transfer
  * @param requestBody user-provided JSON payload describing the transfer
  * @param submittedAt time at which the transfer was pushed to agents
  * @param updatedAt time at which the latest status update was received for the transfer
  * @param reportedInfo JSON output sent to the manager by an agent about the transfer
  * @param stepsRun number of times this transfer has run through a Transporter agent
  */
case class TransferDetails(
  id: UUID,
  status: TransferStatus,
  priority: Short,
  requestBody: Json,
  submittedAt: Option[OffsetDateTime],
  updatedAt: Option[OffsetDateTime],
  reportedInfo: Option[Json],
  stepsRun: Long
)

object TransferDetails {
  implicit val decoder: Decoder[TransferDetails] = deriveDecoder
  implicit val encoder: Encoder[TransferDetails] = deriveEncoder
}
