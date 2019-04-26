package org.broadinstitute.transporter.transfer

import java.time.OffsetDateTime

import io.circe.{Encoder, Json}
import io.circe.derivation.deriveEncoder

/**
  * Summary status for a bulk transfer requests which was submitted
  * to the Transporter manager.
  *
  * @param overallStatus top-level status for the request, derived based on
  *                      the counts of individual statuses in `statusCounts`
  * @param statusCounts counts of the transfers in each potential "transfer status"
  *                     registered under the request
  * @param submittedAt timestamp when the first transfer of the request was submitted
  *                    to Kafka
  * @param updatedAt timestamp of the last message received from Kafka about a transfer
  *                  under this request
  * @param info free-form messages reported by agents after attempting to
  *             perform transfers registered under the request
  */
case class RequestStatus(
  overallStatus: TransferStatus,
  statusCounts: Map[TransferStatus, Long],
  submittedAt: Option[OffsetDateTime],
  updatedAt: Option[OffsetDateTime],
  info: List[Json]
)

object RequestStatus {
  implicit val encoder: Encoder[RequestStatus] = deriveEncoder
}
