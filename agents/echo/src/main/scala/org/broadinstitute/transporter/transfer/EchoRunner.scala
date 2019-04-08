package org.broadinstitute.transporter.transfer

import io.circe.Json
import io.circe.syntax._

/**
  * Example of a custom runner for Transporter agents.
  *
  * Doesn't actually transfer data, so it's useful for manual plumbing tests.
  */
object EchoRunner extends TransferRunner {

  /*--- BOILERPLATE ---*/

  override type In = EchoInput

  override type Progress = EchoInput

  override type Out = String

  override def decodeInput(json: Json): Either[Throwable, EchoInput] = json.as[EchoInput]

  override def decodeProgress(json: Json): Either[Throwable, EchoInput] =
    json.as[EchoInput]

  override def encodeProgress(progress: EchoInput): Json = progress.asJson

  override def encodeOutput(output: String): Json = output.asJson

  /*--- END BOILERPLATE ---*/

  override def initialize(request: EchoInput): EchoInput = request

  override def step(progress: EchoInput): Either[EchoInput, String] = {
    if (progress.fail) {
      throw new IllegalStateException(s"Simulating fatal error during transfer")
    } else {
      Right(progress.message)
    }
  }
}
