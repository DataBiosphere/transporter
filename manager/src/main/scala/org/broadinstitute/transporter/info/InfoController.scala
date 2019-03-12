package org.broadinstitute.transporter.info

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaAdminClient

/**
  * Component responsible for handling status and version requests.
  *
  * @param appVersion current version of Transporter. Typically injected as part of the
  *                   build process, but parameterized here for simpler testing
  * @param dbClient client which can interact with Transporter's DB
  * @param kafkaClient client which can interact with Transporter's Kafka cluster
  * @param cs proof of the ability to shift IO-wrapped computations
  *           onto other threads
  */
class InfoController(
  appVersion: String,
  dbClient: DbClient,
  kafkaClient: KafkaAdminClient
)(
  implicit cs: ContextShift[IO]
) {

  /** Report the current status of Transporter, including the status of its backing systems. */
  def status: IO[ManagerStatus] =
    (dbStatus, kafkaStatus).parMapN {
      case (db, kafka) =>
        ManagerStatus(db.ok && kafka.ok, Map("db" -> db, "kafka" -> kafka))
    }

  /** Report the running version of Transporter. */
  def version: IO[ManagerVersion] = IO.pure(ManagerVersion(appVersion))

  /** Get the current status of Transporter's connection to its DB. */
  private def dbStatus: IO[SystemStatus] =
    dbClient.checkReady.map { ready =>
      SystemStatus(ok = ready, messages = if (ready) Nil else List("Can't connect to DB"))
    }

  /** Get the current status of Transporter's connection to its Kafka cluster. */
  private def kafkaStatus: IO[SystemStatus] =
    kafkaClient.checkReady.map { ready =>
      SystemStatus(
        ok = ready,
        messages = if (ready) Nil else List("Can't connect to Kafka")
      )
    }
}
