package org.broadinstitute.transporter.db

import cats.effect.{ContextShift, IO, Resource}
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor

import scala.concurrent.ExecutionContext

class DbClient private[db] (transactor: Transactor[IO]) {

  def checkReady: IO[Boolean] =
    doobie.FC
      .isValid(0)
      .transact(transactor)
      .attempt
      .map(_.fold(_ => false, identity))
}

object DbClient {
  private val MaxDbConnections = (2 * Runtime.getRuntime.availableProcessors) + 1

  def resource(config: DbConfig, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, DbClient] =
    for {
      transactionContext <- ExecutionContexts.fixedThreadPool[IO](MaxDbConnections)
      transactor <- HikariTransactor.newHikariTransactor[IO](
        config.driverClassname,
        config.connectURL,
        config.username,
        config.password,
        blockingEc,
        transactionContext
      )
    } yield {
      new DbClient(transactor)
    }
}
