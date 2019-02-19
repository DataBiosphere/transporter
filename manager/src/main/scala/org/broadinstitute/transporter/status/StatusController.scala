package org.broadinstitute.transporter.status

import cats.effect.IO
import org.broadinstitute.transporter.db.DbClient

class StatusController(dbClient: DbClient) {

  def status: IO[ManagerStatus] =
    dbStatus.map(db => ManagerStatus(db.ok, Map("db" -> db)))

  private def dbStatus: IO[SystemStatus] =
    dbClient.checkReady.map { ready =>
      SystemStatus(ok = ready, messages = if (ready) Nil else List("Can't connect to DB"))
    }
}
