package org.broadinstitute.transporter.info

import io.circe.Encoder
import io.circe.derivation.deriveEncoder

/**
  * Reported status of the entire agent system.
  *
  * @param ok indication of whether or not the agent is processing requests
  * @param systems individual statuses for each of the agent's backing systems
  */
case class AgentStatus(ok: Boolean, systems: Map[String, SystemStatus])

object AgentStatus {
  implicit val encoder: Encoder[AgentStatus] = deriveEncoder
}
