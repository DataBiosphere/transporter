package org.broadinstitute.transporter

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Configuration for the echo agent.
  *
  * 100% overkill on its own, but useful for experimenting with
  * ways to inject agent-specific config into the generic template.
  *
  * @param transientFailureRate % of the time that the echo agent should ignore
  *                             its input and return a transient failure.
  */
case class EchoConfig(transientFailureRate: Double)

object EchoConfig {
  implicit val reader: ConfigReader[EchoConfig] = deriveReader
}
