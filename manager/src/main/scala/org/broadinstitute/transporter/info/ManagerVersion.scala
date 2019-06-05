package org.broadinstitute.transporter.info

import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

/** Reported version of the Transporter system. */
case class ManagerVersion(version: String)

object ManagerVersion {
  implicit val decoder: Decoder[ManagerVersion] = deriveDecoder
  implicit val encoder: Encoder[ManagerVersion] = deriveEncoder
}
