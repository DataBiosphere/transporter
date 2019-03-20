package org.broadinstitute.transporter.transfer

import io.circe.Encoder
import io.circe.derivation.deriveEncoder

/**
  * Summary status for a bulk transfer requests which was submitted
  * to the Transporter manager.
  *
  * @param overallStatus top-level status for the request, derived based on
  *                      the counts of individual statuses in `statusCounts`
  * @param statusCounts counts of the transfers in each potential "transfer status"
  *                     registered under the request
  */
case class RequestStatus(
  overallStatus: TransferStatus,
  statusCounts: Map[TransferStatus, Long]
)

object RequestStatus {
  implicit val encoder: Encoder[RequestStatus] = deriveEncoder
}
