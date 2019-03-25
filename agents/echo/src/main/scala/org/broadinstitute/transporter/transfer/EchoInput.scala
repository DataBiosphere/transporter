package org.broadinstitute.transporter.transfer

import io.circe.Decoder
import io.circe.derivation.deriveDecoder

case class EchoInput(message: String, fail: Boolean)

object EchoInput {
  implicit val decoder: Decoder[EchoInput] = deriveDecoder
}
