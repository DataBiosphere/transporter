package org.broadinstitute.transporter.transfer.api

import java.util.UUID

import io.circe.{Encoder, Json}
import io.circe.derivation.deriveEncoder

/**
  * Info about a transfer job collected by the manager.
  *
  * @param id unique ID of the transfer within its enclosing request
  * @param message JSON output sent to the manager by an agent about the transfer
  */
case class TransferMessage(id: UUID, message: Json)

object TransferMessage {
  implicit val encoder: Encoder[TransferMessage] = deriveEncoder
}
