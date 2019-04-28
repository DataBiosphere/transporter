package org.broadinstitute.transporter.transfer

import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Description of an attempt to run a transfer.
  *
  * @param result signal describing the success/failure of the attempt
  * @param info optional extra information describing the status signal in more detail
  */
case class TransferSummary[I](result: TransferResult, info: I, id: UUID, requestId: UUID)

object TransferSummary {
  implicit def decoder[I: Decoder]: Decoder[TransferSummary[I]] = deriveDecoder
  implicit def encoder[I: Encoder]: Encoder[TransferSummary[I]] = deriveEncoder
}
