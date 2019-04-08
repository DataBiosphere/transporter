package org.broadinstitute.transporter.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/** Configuration determining how the AWS->GCP agent should authorize with S3. */
case class AwsConfig(
  accessKeyId: String,
  secretAccessKey: String,
  region: String
)

object AwsConfig {
  implicit val reader: ConfigReader[AwsConfig] = deriveReader
}
