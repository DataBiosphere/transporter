package org.broadinstitute.transporter.transfer

/**
  * Example of a custom runner for Transporter agents.
  *
  * Doesn't actually transfer data, so it's useful for manual plumbing tests.
  */
object EchoRunner extends TransferRunner[EchoInput, EchoInput, String] {

  override def initialize(request: EchoInput): Either[EchoInput, String] = Left(request)

  override def step(progress: EchoInput): Either[EchoInput, String] = {
    if (progress.fail) {
      throw new IllegalStateException(s"Simulating fatal error during transfer")
    } else {
      Right(progress.message)
    }
  }
}
