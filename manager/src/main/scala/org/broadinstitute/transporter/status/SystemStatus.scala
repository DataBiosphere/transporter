package org.broadinstitute.transporter.status

import io.circe.Encoder

case class SystemStatus(ok: Boolean, messages: List[String])

object SystemStatus {
  implicit val encoder: Encoder[SystemStatus] = io.circe.derivation.deriveEncoder
}
