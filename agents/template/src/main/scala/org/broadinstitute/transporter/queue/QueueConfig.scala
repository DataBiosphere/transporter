package org.broadinstitute.transporter.queue

import org.http4s.Uri
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Configuration describing how agents should fetch information
  * about the queue of requests they must process.
  *
  * @param managerUri web URI of the Transporter manager service
  *                   storing queue information
  * @param queueName name of the queue to process
  */
case class QueueConfig(managerUri: Uri, queueName: String)

object QueueConfig {
  import pureconfig.module.http4s._
  implicit val reader: ConfigReader[QueueConfig] = deriveReader
}
