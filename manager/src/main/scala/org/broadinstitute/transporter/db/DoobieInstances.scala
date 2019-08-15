package org.broadinstitute.transporter.db

import java.sql.Timestamp
import java.time.{OffsetDateTime, ZoneId}

import cats.implicits._
import doobie.Get
import doobie.postgres.{Instances => PostgresInstances}
import doobie.postgres.circe.Instances.JsonInstances
import io.circe.Json
import org.broadinstitute.transporter.transfer.TransferStatus

object DoobieInstances extends PostgresInstances with JsonInstances {

  implicit val odtGet: Get[OffsetDateTime] = Get[Timestamp].tmap { ts =>
    OffsetDateTime.ofInstant(ts.toInstant, ZoneId.of("UTC"))
  }

  implicit val mapGetter: Get[Map[TransferStatus, Long]] =
    Get[Json].temap(_.hcursor.as[Map[TransferStatus, Long]].leftMap(_.getMessage()))

}
