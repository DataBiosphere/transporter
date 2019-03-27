package org.broadinstitute.transporter.transfer

import cats.effect.IO
import io.circe.syntax._

import scala.util.Random

/**
  * Example of a custom runner for Transporter agents.
  *
  * Doesn't actually transfer data, so it's useful for manual plumbing tests.
  */
class EchoRunner(transientFailureRate: Double) extends TransferRunner[EchoInput] {

  private val rng = new Random()

  override def transfer(request: EchoInput): IO[TransferSummary] =
    IO.delay(rng.nextDouble()).map { rand =>
      val (result, message) = if (rand > transientFailureRate) {
        TransferResult.TransientFailure -> s"Simulating transient value for value $rand"
      } else {
        (if (request.fail) TransferResult.FatalFailure else TransferResult.Success) -> request.message
      }
      TransferSummary(result, Some(message.asJson))
    }
}
