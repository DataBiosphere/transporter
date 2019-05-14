package org.broadinstitute.transporter.queue.api

import io.circe.Decoder
import io.circe.derivation.deriveDecoder
import org.broadinstitute.transporter.queue.QueueSchema

/**
  * Parameters for a queue resource which can be tweaked by users post-creation.
  *
  * @param schema JSON schema which Transporter should enforce for all new
  *               requests submitted to this transfer stream
  * @param maxConcurrentTransfers maximum number of transfers in this queue which
  *                               will be distributed to agents at a time
  * @param partitionCount partitions included in each of the Kafka topics backing
  *                       this queue
  */
case class QueueParameters(
  schema: Option[QueueSchema],
  maxConcurrentTransfers: Option[Int],
  partitionCount: Option[Int]
)

object QueueParameters {
  implicit val decoder: Decoder[QueueParameters] = deriveDecoder
}
