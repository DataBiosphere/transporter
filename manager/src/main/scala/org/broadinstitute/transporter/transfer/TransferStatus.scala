package org.broadinstitute.transporter.transfer

import doobie.postgres.{Instances => PostgresInstances}
import doobie.util.Meta
import enumeratum.EnumEntry.Lowercase
import enumeratum.{CirceEnum, Enum, EnumEntry}
import io.circe.{KeyDecoder, KeyEncoder}

import scala.collection.immutable.IndexedSeq

/**
  * Description of the current status of a transfer tracked by Transporter, as determined
  * by the last result message received from an agent for the transfer (if any).
  */
sealed trait TransferStatus
    extends EnumEntry
    with Lowercase
    with Product
    with Serializable

object TransferStatus
    extends Enum[TransferStatus]
    with CirceEnum[TransferStatus]
    with PostgresInstances {

  override val toString: String = "TransferStatus"

  override val values: IndexedSeq[TransferStatus] = findValues

  implicit val statusMeta: Meta[TransferStatus] =
    pgEnumStringOpt("transfer_status", namesToValuesMap.get, _.entryName)

  implicit val statusEncoder: KeyEncoder[TransferStatus] =
    KeyEncoder.encodeKeyString.contramap(_.entryName)
  implicit val statusDecoder: KeyDecoder[TransferStatus] =
    namesToValuesMap.get

  /** Initial status assigned to all transfers when they are persisted by the manager. */
  case object Pending extends TransferStatus

  /** Status assigned to transfers as they are pushed to downstream agents. */
  case object Submitted extends TransferStatus

  /** Status assigned to transfers when their incremental progress appears in Kafka. */
  case object InProgress extends TransferStatus

  /** Status assigned to transfers which are reported to have failed with a fatal error. */
  case object Failed extends TransferStatus

  /** Status assigned to transfers which are reported to have completed successfully. */
  case object Succeeded extends TransferStatus
}
