package org.broadinstitute.transporter.transfer.api

import java.util.UUID

import io.circe.{Encoder, Json}
import io.circe.derivation.deriveEncoder

case class TransferMessage(id: UUID, message: Json)

object TransferMessage {
  implicit val encoder: Encoder[TransferMessage] = deriveEncoder
}
