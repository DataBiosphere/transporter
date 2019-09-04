package org.broadinstitute.transporter.transfer.api

import io.circe.derivation.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder, Json}

/**
  * A request to launch some number of data transfers.
  *
  * @param payload individual transfer descriptions to track under the request submission.
  *                  The exact schema of each transfer is set in the manager's config on deploy,
  *                  and validated at runtime.
  * @param priority optional integer to indicate priority, where a greater number corresponds to a
  *                 higher priority.
  */
case class TransferRequest(payload: Json, priority: Option[Short])

object TransferRequest {
  implicit val decoder: Decoder[TransferRequest] = deriveDecoder
  implicit val encoder: Encoder[TransferRequest] = deriveEncoder
}
