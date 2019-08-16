package org.broadinstitute.transporter.web

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Generic API envelope for a page of query results.
  *
  * @param items query results to return to the client
  * @param total the total number of items stored across all pages
  */
case class Page[I](items: List[I], total: Long)

object Page {
  implicit def decoder[I: Decoder]: Decoder[Page[I]] = deriveDecoder
  implicit def encoder[I: Encoder]: Encoder[Page[I]] = deriveEncoder
}
