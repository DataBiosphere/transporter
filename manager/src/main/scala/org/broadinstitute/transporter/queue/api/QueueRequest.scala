package org.broadinstitute.transporter.queue.api

import io.circe.Decoder
import org.broadinstitute.transporter.queue.QueueSchema

/**
  * Model for user-provided information required to initialize a queue resource.
  *
  * @param name ID which should be used in future requests when
  *             submitting transfers to the new queue
  * @param schema JSON schema which should be enforced for all requests
  *               submitted to the new queue
  * @param maxConcurrentTransfers maximum number of transfers in the queue which
  *                               should be distributed to agents at a time
  * @param partitionCount number of partitions to initialize in the Kafka topics
  *                       created for the new queue
  */
case class QueueRequest(
  name: String,
  schema: QueueSchema,
  maxConcurrentTransfers: Int,
  partitionCount: Int
)

object QueueRequest {
  implicit val decoder: Decoder[QueueRequest] = io.circe.derivation.deriveDecoder
}
