package org.broadinstitute.transporter.transfer.api

import io.circe.{Decoder, Encoder, Json}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * A request to launch some number of data transfers.
  *
  * @param transfers individual transfer descriptions to track under the request submission.
  *                  The exact schema of each transfer is set in the manager's config on deploy,
  *                  and validated at runtime.
  */
case class BulkRequest(transfers: List[Json])

object BulkRequest {
  implicit val decoder: Decoder[BulkRequest] = deriveDecoder
  implicit val encoder: Encoder[BulkRequest] = deriveEncoder
}
