package org.broadinstitute.transporter.info

import io.circe.Encoder

case class ManagerVersion(version: String)

object ManagerVersion {
  implicit val encoder: Encoder[ManagerVersion] = io.circe.derivation.deriveEncoder
}
