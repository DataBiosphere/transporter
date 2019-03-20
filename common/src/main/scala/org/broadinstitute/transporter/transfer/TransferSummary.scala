package org.broadinstitute.transporter.transfer

import io.circe.{Decoder, Encoder, Json}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Description of an attempt to run a transfer.
  *
  * @param result signal describing the success/failure of the attempt
  * @param info optional extra information describing the status signal in more detail
  */
case class TransferSummary(result: TransferResult, info: Option[Json])

object TransferSummary {
  implicit val decoder: Decoder[TransferSummary] = deriveDecoder
  implicit val encoder: Encoder[TransferSummary] = deriveEncoder
}
