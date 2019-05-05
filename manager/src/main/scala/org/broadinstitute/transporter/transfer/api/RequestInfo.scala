package org.broadinstitute.transporter.transfer.api

import java.util.UUID

import io.circe.Encoder
import io.circe.derivation.deriveEncoder

/**
  * Information about a group of transfer jobs collected by the manager.
  *
  * @param id unique ID of the request which triggered all the transfer jobs
  * @param info job-level information collected by the manager
  */
case class RequestInfo(id: UUID, info: List[TransferInfo])

object RequestInfo {
  implicit val encoder: Encoder[RequestInfo] = deriveEncoder
}
