package org.broadinstitute.transporter.web

import io.circe.Encoder

case class ErrorResponse(message: String)

object ErrorResponse {
  implicit val encoder: Encoder[ErrorResponse] = io.circe.derivation.deriveEncoder
}
