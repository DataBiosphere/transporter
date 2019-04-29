package org.broadinstitute.transporter.transfer.api

import java.time.OffsetDateTime
import java.util.UUID

import io.circe.Encoder
import io.circe.derivation.deriveEncoder
import org.broadinstitute.transporter.transfer.TransferStatus

/**
  * Summary status for a bulk transfer requests which was submitted
  * to the Transporter manager.
  *
  * @param id unique ID of the request within its enclosing queue
  * @param overallStatus top-level status for the request, derived based on
  *                      the counts of individual statuses in `statusCounts`
  * @param statusCounts counts of the transfers in each potential "transfer status"
  *                     registered under the request
  * @param submittedAt timestamp when the first transfer of the request was submitted
  *                    to Kafka
  * @param updatedAt timestamp of the last message received from Kafka about a transfer
  *                  under this request
  */
case class RequestStatus(
  id: UUID,
  overallStatus: TransferStatus,
  statusCounts: Map[TransferStatus, Long],
  submittedAt: Option[OffsetDateTime],
  updatedAt: Option[OffsetDateTime]
)

object RequestStatus {
  implicit val encoder: Encoder[RequestStatus] = deriveEncoder
}
