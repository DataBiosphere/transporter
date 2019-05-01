package org.broadinstitute.transporter.transfer

import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Message describing incremental progress on a running transfer, sent between agents via Kafka.
  *
  * @param progress agent-specified payload describing the current progress of a transfer
  * @param id unique ID for the transfer operation
  * @param requestId unique ID for the bulk request which includes the transfer
  */
case class TransferProgress[P](progress: P, id: UUID, requestId: UUID)

object TransferProgress {
  implicit def decoder[P: Decoder]: Decoder[TransferProgress[P]] = deriveDecoder
  implicit def encoder[P: Encoder]: Encoder[TransferProgress[P]] = deriveEncoder
}
