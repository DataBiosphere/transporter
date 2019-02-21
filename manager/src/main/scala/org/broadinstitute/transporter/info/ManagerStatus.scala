package org.broadinstitute.transporter.info

import io.circe.Encoder

case class ManagerStatus(ok: Boolean, systems: Map[String, SystemStatus])

object ManagerStatus {
  implicit val encoder: Encoder[ManagerStatus] = io.circe.derivation.deriveEncoder
}
