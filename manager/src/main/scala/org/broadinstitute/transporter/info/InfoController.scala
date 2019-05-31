package org.broadinstitute.transporter.info

import cats.effect.{ContextShift, IO}
import cats.implicits._
import doobie.implicits._
import doobie.util.fragment.Fragment
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.db.{Constants, DbLogHandler}

/**
  * Component responsible for handling status and version requests.
  *
  * @param appVersion current version of Transporter. Typically injected as part of the
  *                   build process, but parameterized here for simpler testing
  * @param dbClient client which can interact with Transporter's backing DB
  * @param cs proof of the ability to shift IO-wrapped computations
  *           onto other threads
  */
class InfoController(
  appVersion: String,
  dbClient: Transactor[IO]
)(
  implicit cs: ContextShift[IO]
) {

  private val logger = Slf4jLogger.getLogger[IO]
  private implicit val logHandler: LogHandler = DbLogHandler(logger)

  /** Report the current status of Transporter, including the status of its backing systems. */
  def status: IO[ManagerStatus] =
    dbStatus.map(db => ManagerStatus(db.ok, Map("db" -> db)))

  /** Report the running version of Transporter. */
  def version: IO[ManagerVersion] = IO.pure(ManagerVersion(appVersion))

  /** Get the current status of Transporter's connection to its DB. */
  private def dbStatus: IO[SystemStatus] = {
    val tables =
      List(Constants.RequestsTable, Constants.TransfersTable).map(Fragment.const(_))

    val check = for {
      _ <- logger.info(s"Checking DB tables...")
      // TODO: It should be possible to distinguish "no such table" errors
      // from "can't connect to DB" errors, and give different messages.
      _ <- tables.traverse { t =>
        (fr"select 1 from" ++ t ++ fr"limit 1").query[Int].option
      }.transact(dbClient)
      _ <- logger.debug("Table check succeeded")
    } yield {
      true
    }

    check.handleErrorWith { err =>
      logger.error(err)("DB table check failed").as(false)
    }.map { ready =>
      SystemStatus(ok = ready, messages = if (ready) Nil else List("Can't connect to DB"))
    }
  }
}
