package org.broadinstitute.transporter.web

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

case class Page[I](items: List[I], total: Long)

object Page {
  implicit def decoder[I: Decoder]: Decoder[Page[I]] = deriveDecoder
  implicit def encoder[I: Encoder]: Encoder[Page[I]] = deriveEncoder
}
