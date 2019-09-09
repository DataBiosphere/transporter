package org.broadinstitute.transporter.db

import java.time.Instant

import cats.implicits._
import doobie._
import doobie.implicits._

object Constants {

  val RequestsTable = "transfer_requests"
  val TransfersTable = "transfers"

  /** SQL fragment linking transfer-level information to request-level information. */
  val TransfersJoinTable: Fragment = List(
    Fragment.const(TransfersTable),
    fr"t JOIN",
    Fragment.const(RequestsTable),
    fr"r ON t.request_id = r.id"
  ).combineAll

  def timestampSql(now: Instant): String =
    s"TO_TIMESTAMP(${now.toEpochMilli}::double precision / 1000)"
}
