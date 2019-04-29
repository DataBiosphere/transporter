package org.broadinstitute.transporter.queue.api

import io.circe.derivation.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.broadinstitute.transporter.queue.QueueSchema

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
  * @param maxConcurrentTransfers maximum number of transfers in this queue which
  *                               will be distributed to agents at a time
  */
case class Queue(
  name: String,
  requestTopic: String,
  progressTopic: String,
  responseTopic: String,
  schema: QueueSchema,
  maxConcurrentTransfers: Int
)

object Queue {
  implicit val encoder: Encoder[Queue] = deriveEncoder
  implicit val decoder: Decoder[Queue] = deriveDecoder
}
