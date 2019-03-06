package org.broadinstitute.transporter.queue

import org.http4s.Uri
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class QueueConfig(managerUri: Uri, queueName: String)

object QueueConfig {
  import pureconfig.module.http4s._
  implicit val reader: ConfigReader[QueueConfig] = deriveReader
}
