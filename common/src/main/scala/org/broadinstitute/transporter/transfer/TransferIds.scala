package org.broadinstitute.transporter.transfer

import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Collection of IDs uniquely identifying a single transfer request.
  *
  * @param request unique ID of the request which launched the transfer
  * @param transfer ID of the transfer; unique within the triggering request
  */
case class TransferIds(request: UUID, transfer: UUID)

object TransferIds {
  implicit val decoder: Decoder[TransferIds] = deriveDecoder
  implicit val encoder: Encoder[TransferIds] = deriveEncoder
}
