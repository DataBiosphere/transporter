package org.broadinstitute.transporter.transfer

import io.circe.{Decoder, Json}

/**
  * A request to launch some number of data transfers under a queue resource.
  *
  * @param transfers individual transfer descriptions to track under the request
  *                  submission. The exact schema of each transfer is set (and validated)
  *                  on a per-queue basis
  */
case class TransferRequest(transfers: List[Json])

object TransferRequest {
  implicit val decoder: Decoder[TransferRequest] = io.circe.derivation.deriveDecoder
}
