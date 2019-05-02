package org.broadinstitute.transporter.transfer

import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Description of an attempt to run a transfer, sent by agents to the manager via Kafka.
  *
  * @param result signal describing the success/failure of the attempt
  * @param info agent-specified payload describing either 1) outputs of the successful transfer
  *             or 2) details of the error which caused the transfer to fail
  * @param id unique ID for the transfer operation
  * @param requestId unique ID for the bulk request which includes the transfer
  */
case class TransferSummary[I](result: TransferResult, info: I, id: UUID, requestId: UUID)

object TransferSummary {
  implicit def decoder[I: Decoder]: Decoder[TransferSummary[I]] = deriveDecoder
  implicit def encoder[I: Encoder]: Encoder[TransferSummary[I]] = deriveEncoder
}
