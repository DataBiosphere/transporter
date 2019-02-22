package org.broadinstitute.transporter.info

import io.circe.Encoder

/**
  * Reported status of a system backing Transporter.
  *
  * @param ok indication of whether or not Transporter can interact with the system
  * @param messages more information describing the current state of the system
  *
  * @see https://docs.google.com/document/d/1G-dnaxxZQ0m9KNEtYnf5sddVvjJQmj40J6a0JSQJwZU/edit#heading=h.m0jtmpxcr9vp
  *      for Analysis Platform's specification of this payload
  */
case class SystemStatus(ok: Boolean, messages: List[String])

object SystemStatus {
  implicit val encoder: Encoder[SystemStatus] = io.circe.derivation.deriveEncoder
}
