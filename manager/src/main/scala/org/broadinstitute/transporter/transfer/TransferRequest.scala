package org.broadinstitute.transporter.transfer

import io.circe.{Decoder, Json}

case class TransferRequest(transfers: List[Json])

object TransferRequest {
  implicit val decoder: Decoder[TransferRequest] = io.circe.derivation.deriveDecoder
}
