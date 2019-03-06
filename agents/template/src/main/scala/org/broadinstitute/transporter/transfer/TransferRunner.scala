package org.broadinstitute.transporter.transfer

trait TransferRunner[Req] {
  def transfer(request: Req): TransferResult
}
