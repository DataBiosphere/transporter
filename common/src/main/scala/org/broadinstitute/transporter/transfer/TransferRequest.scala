package org.broadinstitute.transporter.transfer

import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Trigger message for a transfer, sent by the manager to agents via Kafka.
  *
  * @param transfer user-provided request body describing the transfer to perform
  * @param id unique ID for the transfer operation
  * @param requestId unique ID for the bulk request which includes the transfer
  */
case class TransferRequest[R](transfer: R, id: UUID, requestId: UUID)

object TransferRequest {
  implicit def decoder[R: Decoder]: Decoder[TransferRequest[R]] = deriveDecoder
  implicit def encoder[R: Encoder]: Encoder[TransferRequest[R]] = deriveEncoder
}
