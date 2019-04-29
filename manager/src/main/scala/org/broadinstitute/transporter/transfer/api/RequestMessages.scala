package org.broadinstitute.transporter.transfer.api

import java.util.UUID

import io.circe.Encoder
import io.circe.derivation.deriveEncoder

case class RequestMessages(id: UUID, messages: List[TransferMessage])

object RequestMessages {
  implicit val encoder: Encoder[RequestMessages] = deriveEncoder
}
