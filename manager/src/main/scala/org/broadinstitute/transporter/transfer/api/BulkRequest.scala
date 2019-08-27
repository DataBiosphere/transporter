package org.broadinstitute.transporter.transfer.api

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * A request to launch some number of data transfers.
  *
  * @param transfers individual transfer descriptions to track under the request submission.
  *                  The exact schema of each transfer is set in the manager's config on deploy,
  *                  and validated at runtime.
  * @param defaults  optional TransferRequest that sets defaults for all transfers under a request.
  */
case class BulkRequest(
  transfers: List[TransferRequest],
  defaults: Option[TransferRequest] = None
)

object BulkRequest {
  implicit val decoder: Decoder[BulkRequest] = deriveDecoder
  implicit val encoder: Encoder[BulkRequest] = deriveEncoder
}
