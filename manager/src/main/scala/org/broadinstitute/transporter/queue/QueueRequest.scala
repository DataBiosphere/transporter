package org.broadinstitute.transporter.queue

import io.circe.Decoder

case class QueueRequest(name: String, schema: QueueSchema)

object QueueRequest {
  implicit val decoder: Decoder[QueueRequest] = io.circe.derivation.deriveDecoder
}
