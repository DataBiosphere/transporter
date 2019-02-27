package org.broadinstitute.transporter.queue

import io.circe.Encoder

case class Queue(
  name: String,
  requestTopic: String,
  responseTopic: String,
  schema: QueueSchema
)

object Queue {
  implicit val encoder: Encoder[Queue] = io.circe.derivation.deriveEncoder
}
