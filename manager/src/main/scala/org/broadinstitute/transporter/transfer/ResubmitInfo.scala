package org.broadinstitute.transporter.transfer

import io.chrisdavenport.fuuid.FUUID
import io.circe.Json

/**
  * Container for information needed to resubmit a transfer.
  *
  * @param transferId unique ID for the transfer
  * @param requestTopic topic to which the transfer should be re-submitted
  * @param transferBody JSON body of the transfer
  */
case class ResubmitInfo(transferId: FUUID, requestTopic: String, transferBody: Json)
