package org.broadinstitute.transporter.transfer

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Message sent between Transporter components over Kafka to
  * describe some stage of the transfer process.
  *
  * @param ids collection of data uniquely identifying the transfer
  * @param message data describing an update for the process
  */
case class TransferMessage[M](ids: TransferIds, message: M)

object TransferMessage {
  implicit def decoder[M: Decoder]: Decoder[TransferMessage[M]] = deriveDecoder
  implicit def encoder[M: Encoder]: Encoder[TransferMessage[M]] = deriveEncoder
}
