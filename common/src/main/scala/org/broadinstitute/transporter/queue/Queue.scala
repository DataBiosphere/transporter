package org.broadinstitute.transporter.queue

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Model of a distinct stream of transfer requests which can be submitted
  * to / distributed / tracked by Transporter.
  *
  * @param name ID provided by users when submitting requests to the
  *             transfer stream
  * @param requestTopic Kafka topic which Transporter should push new transfer
  *                     requests onto for this queue
  * @param progressTopic Kafka topic from which Transporter should read incremental
  *                      progress updates for transfers in this queue
  * @param responseTopic Kafka topic from which Transporter should read final
  *                      transfer summaries for transfers in this queue
  * @param schema JSON schema which Transporter should enforce for all new
  *               requests submitted to this transfer stream
  */
case class Queue(
  name: String,
  requestTopic: String,
  progressTopic: String,
  responseTopic: String,
  schema: QueueSchema
)

object Queue {
  implicit val encoder: Encoder[Queue] = deriveEncoder
  implicit val decoder: Decoder[Queue] = deriveDecoder
}
