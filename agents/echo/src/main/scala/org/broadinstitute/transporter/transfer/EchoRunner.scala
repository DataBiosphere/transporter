package org.broadinstitute.transporter.transfer

import io.circe.Json
import io.circe.syntax._

import scala.util.Random

/**
  * Example of a custom runner for Transporter agents.
  *
  * Doesn't actually transfer data, so it's useful for manual plumbing tests.
  */
class EchoRunner(transientFailureRate: Double) extends TransferRunner {
  import EchoRunner._

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

  private val rng = new Random()

  override def initialize(request: EchoInput): EchoInput = {
    val rand = rng.nextDouble()

    if (rand > transientFailureRate) {
      throw SimulatedFlakyError(rand)
    } else {
      request
    }
  }

  override def step(progress: EchoInput): Either[EchoInput, String] = {
    val rand = rng.nextDouble()

    if (rand > transientFailureRate) {
      throw SimulatedFlakyError(rand)
    } else if (progress.fail) {
      throw new IllegalStateException(s"Simulating fatal error during transfer")
    } else {
      Right(progress.message)
    }
  }

  override def retriable(err: Throwable): Boolean =
    err.isInstanceOf[SimulatedFlakyError]
}

object EchoRunner {

  case class SimulatedFlakyError(pct: Double)
      extends RuntimeException(s"Simulating transient failure for value $pct")
}
