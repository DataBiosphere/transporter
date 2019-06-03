package org.broadinstitute.transporter.transfer.api

import io.circe.{Decoder, Json}

/**
  * A request to launch some number of data transfers.
  *
  * @param transfers individual transfer descriptions to track under the request submission.
  *                  The exact schema of each transfer is set in the manager's config on deploy,
  *                  and validated at runtime.
  */
case class BulkRequest(transfers: List[Json])

object BulkRequest {
  implicit val decoder: Decoder[BulkRequest] = io.circe.derivation.deriveDecoder
}
