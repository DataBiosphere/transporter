package org.broadinstitute.transporter.transfer

/**
  * Component capable of actually running data transfers.
  *
  * Agents are expected to "fill in" an instance of this interface
  * to handle specific storage source / destination pairs.
  */
trait TransferRunner[Req] {

  /**
    * Run the transfer described by the given request.
    */
  def transfer(request: Req): TransferResult
}
