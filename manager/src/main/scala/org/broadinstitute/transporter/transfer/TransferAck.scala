package org.broadinstitute.transporter.transfer

import java.util.UUID

import io.circe.Encoder

case class TransferAck(id: UUID)

object TransferAck {
  implicit val encoder: Encoder[TransferAck] = io.circe.derivation.deriveEncoder
}
