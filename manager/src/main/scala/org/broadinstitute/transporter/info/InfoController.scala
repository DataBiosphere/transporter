package org.broadinstitute.transporter.info

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaClient

class InfoController(appVersion: String, dbClient: DbClient, kafkaClient: KafkaClient)(
  implicit cs: ContextShift[IO]
) {

  def status: IO[ManagerStatus] =
    (dbStatus, kafkaStatus).parMapN {
      case (db, kafka) =>
        ManagerStatus(db.ok && kafka.ok, Map("db" -> db, "kafka" -> kafka))
    }

  def version: IO[ManagerVersion] = IO.pure(ManagerVersion(appVersion))

  private def dbStatus: IO[SystemStatus] =
    dbClient.checkReady.map { ready =>
      SystemStatus(ok = ready, messages = if (ready) Nil else List("Can't connect to DB"))
    }

  private def kafkaStatus: IO[SystemStatus] =
    kafkaClient.checkReady.map { ready =>
      SystemStatus(
        ok = ready,
        messages = if (ready) Nil else List("Can't connect to Kafka")
      )
    }
}
