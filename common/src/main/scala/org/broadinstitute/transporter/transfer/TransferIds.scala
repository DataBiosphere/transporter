package org.broadinstitute.transporter.transfer

import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Collection of IDs uniquely identifying a single transfer request.
  *
  * @param queue globally unique ID of the queue containing the transfer
  * @param request ID of the request which launched the transfer; unique within the enclosing queue
  * @param transfer ID of the transfer; unique within the triggering request
  */
case class TransferIds(queue: UUID, request: UUID, transfer: UUID)

object TransferIds {
  implicit val decoder: Decoder[TransferIds] = deriveDecoder
  implicit val encoder: Encoder[TransferIds] = deriveEncoder
}
