package org.broadinstitute.transporter.transfer

import doobie.postgres.{Instances => PostgresInstances}
import doobie.util.Meta
import enumeratum.EnumEntry.Lowercase
import enumeratum.{CirceEnum, Enum, EnumEntry}
import io.circe.KeyEncoder

import scala.collection.immutable.IndexedSeq

sealed trait TransferStatus
    extends EnumEntry
    with Lowercase
    with Product
    with Serializable

object TransferStatus
    extends Enum[TransferStatus]
    with CirceEnum[TransferStatus]
    with PostgresInstances {

  override val values: IndexedSeq[TransferStatus] = findValues

  implicit val statusMeta: Meta[TransferStatus] =
    pgEnumStringOpt("transfer_status", namesToValuesMap.get, _.entryName)

  implicit val statusEncoder: KeyEncoder[TransferStatus] =
    KeyEncoder.encodeKeyString.contramap(_.entryName)

  case object Submitted extends TransferStatus
  case object Retrying extends TransferStatus
  case object Failed extends TransferStatus
  case object Succeeded extends TransferStatus
}
