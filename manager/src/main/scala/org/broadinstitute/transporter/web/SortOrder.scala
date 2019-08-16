package org.broadinstitute.transporter.web

import enumeratum.{Enum, EnumEntry}
import enumeratum.EnumEntry.Lowercase

import scala.collection.immutable.IndexedSeq

/**
  * Enum modeling valid sort orders for lookup requests.
  *
  * Scoped at the `web` layer because it's mainly useful for
  * generating detailed OpenAPI documentation.
  */
sealed trait SortOrder extends EnumEntry with Lowercase

object SortOrder extends Enum[SortOrder] {
  override val values: IndexedSeq[SortOrder] = findValues

  case object Asc extends SortOrder
  case object Desc extends SortOrder
}
