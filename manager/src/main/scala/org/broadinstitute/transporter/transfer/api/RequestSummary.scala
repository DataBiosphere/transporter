package org.broadinstitute.transporter.transfer.api

import java.time.OffsetDateTime
import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}
import org.broadinstitute.transporter.transfer.TransferStatus

/**
  * Summary status for a bulk transfer requests which was submitted
  * to the Transporter manager.
  *
  * @param id unique ID of the request
  * @param receivedAt timestamp when the request batch was stored by Transporter
  * @param statusCounts counts of the transfers in each potential "transfer status"
  *                     registered under the request
  * @param submittedAt timestamp when the first transfer of the request was submitted
  *                    to Kafka
  * @param updatedAt timestamp of the last message received from Kafka about a transfer
  *                  under this request
  */
case class RequestSummary(
  id: UUID,
  receivedAt: OffsetDateTime,
  statusCounts: Map[TransferStatus, Long],
  submittedAt: Option[OffsetDateTime],
  updatedAt: Option[OffsetDateTime]
)

object RequestSummary {
  implicit val decoder: Decoder[RequestSummary] = deriveDecoder
  implicit val encoder: Encoder[RequestSummary] = deriveEncoder
}
