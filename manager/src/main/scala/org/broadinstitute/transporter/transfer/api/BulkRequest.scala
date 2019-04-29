package org.broadinstitute.transporter.transfer.api

import io.circe.{Decoder, Json}

/**
  * A request to launch some number of data transfers under a queue resource.
  *
  * @param transfers individual transfer descriptions to track under the request submission.
  *                  The exact schema of each transfer is set (and validated) on a per-queue basis
  */
case class BulkRequest(transfers: List[Json])

object BulkRequest {
  implicit val decoder: Decoder[BulkRequest] = io.circe.derivation.deriveDecoder
}
