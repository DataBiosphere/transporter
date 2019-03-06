package org.broadinstitute.transporter.transfer

import io.circe.{Decoder, Encoder, JsonObject}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

case class TransferResult(status: TransferStatus, info: Option[JsonObject])

object TransferResult {
  implicit val decoder: Decoder[TransferResult] = deriveDecoder
  implicit val encoder: Encoder[TransferResult] = deriveEncoder
}
