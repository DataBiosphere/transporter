package org.broadinstitute.transporter.transfer.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration.FiniteDuration

case class TransferConfig(
  schema: TransferSchema,
  maxInFlight: Long,
  submissionInterval: FiniteDuration
)

object TransferConfig {
  implicit val reader: ConfigReader[TransferConfig] = deriveReader
}
