package org.broadinstitute.transporter.info

import io.circe.Encoder
import io.circe.derivation.deriveEncoder

/**
  * Reported status of a system backing the agent.
  *
  * @param ok indication of whether or not the system is healthy
  * @param messages more information describing the current state of the system
  */
case class SystemStatus(ok: Boolean, messages: List[String])

object SystemStatus {
  implicit val encoder: Encoder[SystemStatus] = deriveEncoder
}
