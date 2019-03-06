package org.broadinstitute.transporter.transfer

trait TransferRunner[In] {
  def transfer(request: In): TransferResult
}
