package org.broadinstitute.transporter.info

import io.circe.Encoder

/**
  * Reported status of the entire Transporter system.
  *
  * @param ok indication of whether or not Transporter can handle requests
  * @param systems individual statuses for each of Transporter's backing systems
  *
  * @see https://docs.google.com/document/d/1G-dnaxxZQ0m9KNEtYnf5sddVvjJQmj40J6a0JSQJwZU/edit#heading=h.m0jtmpxcr9vp
  *      for Analysis Platform's specification of this payload
  */
case class ManagerStatus(ok: Boolean, systems: Map[String, SystemStatus])

object ManagerStatus {
  implicit val encoder: Encoder[ManagerStatus] = io.circe.derivation.deriveEncoder
}
