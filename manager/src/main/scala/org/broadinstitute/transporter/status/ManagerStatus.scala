package org.broadinstitute.transporter.status

import io.circe.Encoder

case class ManagerStatus(ok: Boolean, systems: Map[String, SystemStatus])

object ManagerStatus {
  implicit val encoder: Encoder[ManagerStatus] = io.circe.derivation.deriveEncoder
}
