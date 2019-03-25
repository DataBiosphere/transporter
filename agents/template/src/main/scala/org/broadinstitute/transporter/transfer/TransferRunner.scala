package org.broadinstitute.transporter.transfer

import cats.effect.IO

/**
  * Component capable of actually running data transfers.
  *
  * Agents are expected to "fill in" an instance of this interface
  * to handle specific storage source / destination pairs.
  */
abstract class TransferRunner[R] {

  /**
    * Run the transfer described by the given request.
    */
  def transfer(request: R): IO[TransferSummary]
}
