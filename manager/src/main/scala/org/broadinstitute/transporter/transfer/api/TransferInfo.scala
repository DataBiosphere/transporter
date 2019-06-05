package org.broadinstitute.transporter.transfer.api

import java.util.UUID

import io.circe.{Decoder, Encoder, Json}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Info about a transfer job collected by the manager.
  *
  * @param id unique ID of the transfer within its enclosing request
  * @param info JSON output sent to the manager by an agent about the transfer
  */
case class TransferInfo(id: UUID, info: Json)

object TransferInfo {
  implicit val decoder: Decoder[TransferInfo] = deriveDecoder
  implicit val encoder: Encoder[TransferInfo] = deriveEncoder
}
