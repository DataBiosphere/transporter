package org.broadinstitute.transporter.transfer

import io.circe.Json

/**
  * Component capable of actually running data transfers.
  *
  * Agents are expected to "fill in" an instance of this interface
  * to handle specific storage source / destination pairs.
  */
trait TransferRunner {

  /**
    * Run the transfer described by the given request.
    */
  def transfer(request: Json): TransferSummary
}
