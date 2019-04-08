package org.broadinstitute.transporter.config

import java.nio.file.Path

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/** Configuration determining how the AWS->GCP agent should connect to GCS. */
case class GcpConfig(serviceAccountJson: Option[Path])

object GcpConfig {
  implicit val reader: ConfigReader[GcpConfig] = deriveReader
}
