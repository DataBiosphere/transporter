package org.broadinstitute.transporter.transfer

import java.util.UUID

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/**
  * Convenience case class for the TransferListener's expansion processing.
  *
  * @param expandedTo list of new transfer IDs that an original transfer is expanded to
  */
case class ExpandedTransferIds(expandedTo: List[UUID])

object ExpandedTransferIds {
  implicit val decoder: Decoder[ExpandedTransferIds] = deriveDecoder
  implicit val encoder: Encoder[ExpandedTransferIds] = deriveEncoder
}
