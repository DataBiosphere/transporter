package org.broadinstitute.transporter.queue

import io.circe.Encoder

/**
  * Model of a distinct stream of transfer requests which can be submitted
  * to / distributed / tracked by Transporter.
  *
  * @param name ID provided by users when submitting requests to the
  *             transfer stream
  * @param requestTopic Kafka topic which Transporter pushes new transfer
  *                     requests onto for this stream
  * @param responseTopic Kafka topic which Transporter reads transfer
  *                      updates from for this stream
  * @param schema JSON schema which Transporter should enforce for all new
  *               requests submitted to this transfer stream
  */
case class Queue(
  name: String,
  requestTopic: String,
  responseTopic: String,
  schema: QueueSchema
)

object Queue {
  implicit val encoder: Encoder[Queue] = io.circe.derivation.deriveEncoder
}
