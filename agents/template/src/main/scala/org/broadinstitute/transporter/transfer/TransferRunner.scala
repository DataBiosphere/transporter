package org.broadinstitute.transporter.transfer

import org.broadinstitute.transporter.kafka.TransferStep

/**
  * Component capable of actually running data transfers.
  *
  * Agents are expected to "fill in" an instance of this interface
  * to handle specific storage source / destination pairs.
  */
trait TransferRunner[I, P, O] {
  /**
    * Initialize the transfer described by the given request, and
    * emit enough information to push the first chunk of data.
    */
  def initialize(request: I): TransferStep[I, P, O]

  /**
    * Push the next chunk of data into an initialized transfer, either
    * completing the transfer or emitting enough information to push the
    * following chunk.
    */
  def step(progress: P): TransferStep[Nothing, P, O]
}
