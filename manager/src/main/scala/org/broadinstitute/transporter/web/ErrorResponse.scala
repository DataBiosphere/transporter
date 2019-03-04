package org.broadinstitute.transporter.web

import io.circe.Encoder

/**
  * Model returned to clients over HTTP when Transporter
  * encounters any error in processing a request.
  */
case class ErrorResponse(message: String)

object ErrorResponse {
  implicit val encoder: Encoder[ErrorResponse] = io.circe.derivation.deriveEncoder
}
