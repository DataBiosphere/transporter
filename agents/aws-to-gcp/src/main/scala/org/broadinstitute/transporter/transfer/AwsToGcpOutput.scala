package org.broadinstitute.transporter.transfer

import io.circe.Encoder
import io.circe.derivation.deriveEncoder

case class AwsToGcpOutput(gcsBucket: String, gcsPath: String)

object AwsToGcpOutput {
  implicit val encoder: Encoder[AwsToGcpOutput] = deriveEncoder
}
