package org.broadinstitute.transporter.db

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.concurrent.{ExecutionContext, TimeoutException}

class DbClient private[db] (transactor: Transactor[IO])(
  implicit cs: ContextShift[IO]
) {

  private val logger = Slf4jLogger.getLogger[IO]

  def checkReady: IO[Boolean] = {
    val check = for {
      _ <- logger.info("Running status check against DB...")
      isValid <- doobie.FC.isValid(0).transact(transactor)
    } yield {
      isValid
    }

    check.handleErrorWith {
      case _: TimeoutException =>
        logger.error("DB status check timed out!").as(false)
      case err =>
        logger.error(err)("DB status check hit error").as(false)
    }
  }
}

object DbClient {
  private val MaxDbConnections = (2 * Runtime.getRuntime.availableProcessors) + 1

  def resource(config: DbConfig, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, DbClient] =
    for {
      transactionContext <- ExecutionContexts.fixedThreadPool[IO](MaxDbConnections)
      _ <- Resource.liftF(IO.delay(Class.forName(config.driverClassname)))
      transactor <- HikariTransactor.initial[IO](blockingEc, transactionContext)
      _ <- Resource.liftF {
        transactor.configure { dataSource =>
          IO.delay {
            // Basic connection config:
            dataSource.setJdbcUrl(config.connectURL)
            dataSource.setUsername(config.username)
            dataSource.setPassword(config.password)

            // Turn knobs here:
            dataSource.setMaximumPoolSize(MaxDbConnections)
            dataSource.setConnectionTimeout(config.timeouts.connectionTimeout.toMillis)
            dataSource.setMaxLifetime(config.timeouts.maxConnectionLifetime.toMillis)
            dataSource.setValidationTimeout(
              config.timeouts.connectionValidationTimeout.toMillis
            )
            dataSource.setLeakDetectionThreshold(
              config.timeouts.leakDetectionThreshold.toMillis
            )
          }
        }
      }
    } yield {
      new DbClient(transactor)
    }
}
