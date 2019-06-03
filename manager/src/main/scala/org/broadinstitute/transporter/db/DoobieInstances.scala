package org.broadinstitute.transporter.db

import java.sql.Timestamp
import java.time.{OffsetDateTime, ZoneId}

import doobie.postgres.{Instances => PostgresInstances}
import doobie.postgres.circe.Instances.JsonInstances
import doobie.util.Get

object DoobieInstances extends PostgresInstances with JsonInstances {

  implicit val odtGet: Get[OffsetDateTime] = Get[Timestamp].map { ts =>
    OffsetDateTime.ofInstant(ts.toInstant, ZoneId.of("UTC"))
  }
}
