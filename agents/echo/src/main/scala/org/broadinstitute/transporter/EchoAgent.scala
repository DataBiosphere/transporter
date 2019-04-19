package org.broadinstitute.transporter

import cats.effect.{IO, Resource}
import org.broadinstitute.transporter.transfer.{EchoInput, EchoRunner, TransferRunner}

/**
  * Transporter agent which doesn't actually transfer data,
  * but instead echoes its inputs back to the Manager.
  *
  * Useful for manual plumbing tests.
  */
object EchoAgent extends TransporterAgent[EchoConfig, EchoInput, EchoInput, String] {

  override def runnerResource(
    config: EchoConfig
  ): Resource[IO, TransferRunner[EchoInput, EchoInput, String]] =
    Resource.pure(EchoRunner)
}
