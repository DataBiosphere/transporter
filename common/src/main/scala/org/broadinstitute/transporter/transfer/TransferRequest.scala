package org.broadinstitute.transporter.transfer

import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

case class TransferRequest[R](transfer: R, id: UUID, requestId: UUID)

object TransferRequest {
  implicit def decoder[R: Decoder]: Decoder[TransferRequest[R]] = deriveDecoder
  implicit def encoder[R: Encoder]: Encoder[TransferRequest[R]] = deriveEncoder
}
