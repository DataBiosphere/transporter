package org.broadinstitute.transporter.transfer

import enumeratum.EnumEntry.UpperSnakecase
import enumeratum.{CirceEnum, Enum, EnumEntry}

import scala.collection.immutable.IndexedSeq

sealed trait TransferStatus extends EnumEntry

object TransferStatus
    extends Enum[TransferStatus]
    with CirceEnum[TransferStatus]
    with UpperSnakecase {

  override val values: IndexedSeq[TransferStatus] = findValues

  case object Success extends TransferStatus
  case object RetriableFailure extends TransferStatus
  case object FatalFailure extends TransferStatus
}
