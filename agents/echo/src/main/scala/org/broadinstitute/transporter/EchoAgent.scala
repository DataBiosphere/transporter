package org.broadinstitute.transporter

import cats.effect.{IO, Resource}
import org.broadinstitute.transporter.transfer.{EchoRunner, TransferRunner}

/**
  * Transporter agent which doesn't actually transfer data,
  * but instead echoes its inputs back to the Manager.
  *
  * Useful for manual plumbing tests.
  */
object EchoAgent extends TransporterAgent[EchoConfig] {

  override def runnerResource(config: EchoConfig): Resource[IO, TransferRunner] =
    Resource.pure(EchoRunner)
}
