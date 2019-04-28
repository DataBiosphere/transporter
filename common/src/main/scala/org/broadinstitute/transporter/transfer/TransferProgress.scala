package org.broadinstitute.transporter.transfer

import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

case class TransferProgress[P](progress: P, id: UUID, requestId: UUID)

object TransferProgress {
  implicit def decoder[P: Decoder]: Decoder[TransferProgress[P]] = deriveDecoder
  implicit def encoder[P: Encoder]: Encoder[TransferProgress[P]] = deriveEncoder
}
