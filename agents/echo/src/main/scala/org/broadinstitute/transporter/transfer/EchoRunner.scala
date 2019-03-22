package org.broadinstitute.transporter.transfer

import cats.effect.IO
import cats.implicits._
import io.circe.{Decoder, Json}
import io.circe.derivation.deriveDecoder
import io.circe.syntax._
import org.broadinstitute.transporter.EchoConfig

import scala.util.Random

/**
  * Example of a custom runner for Transporter agents.
  *
  * Doesn't actually transfer data, so it's useful for manual plumbing tests.
  */
class EchoRunner(config: EchoConfig) extends TransferRunner(config) {

  private val rng = new Random()

  override def transfer(request: Json): IO[TransferSummary] =
    for {
      rand <- IO.delay(rng.nextDouble())
      (result, message) <- if (rand > config.transientFailureRate) {
        IO.pure {
          TransferResult.TransientFailure -> s"Simulating transient value for value $rand"
        }
      } else {
        request
          .as[EchoRunner.Input]
          .map { input =>
            (
              if (input.fail) TransferResult.FatalFailure else TransferResult.Success,
              input.message
            )
          }
          .liftTo[IO]
      }
    } yield {
      TransferSummary(result, Some(message.asJson))
    }
}

object EchoRunner {

  case class Input(message: String, fail: Boolean)

  implicit val decoder: Decoder[Input] = deriveDecoder
}
