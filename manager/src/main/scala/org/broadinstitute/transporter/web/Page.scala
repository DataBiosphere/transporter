package org.broadinstitute.transporter.web

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Generic API envelope for a page of query results.
  *
  * @param items query results to return to the client
  * @param total the number of elements included in `items`
  */
case class Page[I](items: List[I], total: Int)

object Page {
  implicit def decoder[I: Decoder]: Decoder[Page[I]] = deriveDecoder
  implicit def encoder[I: Encoder]: Encoder[Page[I]] = deriveEncoder
}
