package org.broadinstitute.transporter.queue

import io.circe.Decoder

/**
  * Model for user-provided information required to initialize a queue resource.
  *
  * @param name ID which should be used in future requests when
  *             submitting transfers to the new queue
  * @param schema JSON schema which should be enforced for all requests
  *               submitted to the new queue
  */
case class QueueRequest(name: String, schema: QueueSchema)

object QueueRequest {
  implicit val decoder: Decoder[QueueRequest] = io.circe.derivation.deriveDecoder
}
