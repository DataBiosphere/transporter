package org.broadinstitute.transporter.status

import cats.effect.IO
import org.broadinstitute.transporter.db.DbClient

class StatusController(dbClient: DbClient) {

  def status: IO[String] = dbClient.checkReady.map { ready =>
    if (ready) "It's alive" else "Oh no!"
  }
}
