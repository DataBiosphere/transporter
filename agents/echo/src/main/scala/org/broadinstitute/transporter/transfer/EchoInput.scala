package org.broadinstitute.transporter.transfer

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

case class EchoInput(message: String, fail: Boolean)

object EchoInput {
  implicit val decoder: Decoder[EchoInput] = deriveDecoder
  implicit val encoder: Encoder[EchoInput] = deriveEncoder
}
