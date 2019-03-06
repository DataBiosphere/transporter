package org.broadinstitute.transporter.transfer

import enumeratum.EnumEntry.UpperSnakecase
import enumeratum.{CirceEnum, Enum, EnumEntry}

import scala.collection.immutable.IndexedSeq

/**
  * Signal sent from Transporter agents to the manager describing the
  * result of an attempt to run a transfer request.
  */
sealed trait TransferStatus extends EnumEntry

object TransferStatus
    extends Enum[TransferStatus]
    with CirceEnum[TransferStatus]
    with UpperSnakecase {

  override val values: IndexedSeq[TransferStatus] = findValues

  /** Signal for transfers that completed successfully. */
  case object Success extends TransferStatus

  /**
    * Signal for transfers that failed with a transient error and
    * are likely to succeed if resubmitted.
    *
    * Whether or not a failure should be considered retriable is
    * left up to the individual agent implementations.
    */
  case object RetriableFailure extends TransferStatus

  /**
    * Signal for transfers that failed and are likely / guaranteed
    * to fail again if resubmitted.
    *
    * Agents can choose to manually return this error with descriptive
    * info. Uncaught exceptions during transfer processing also result
    * in this status being sent.
    */
  case object FatalFailure extends TransferStatus
}
