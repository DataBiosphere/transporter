package org.broadinstitute.transporter.transfer

import io.circe.{Decoder, Encoder, JsonObject}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Description of an attempt to run a transfer.
  *
  * @param status signal describing the success/failure of the attempt
  * @param info optional extra information describing the status signal in more detail
  */
case class TransferResult(status: TransferStatus, info: Option[JsonObject])

object TransferResult {
  implicit val decoder: Decoder[TransferResult] = deriveDecoder
  implicit val encoder: Encoder[TransferResult] = deriveEncoder
}
