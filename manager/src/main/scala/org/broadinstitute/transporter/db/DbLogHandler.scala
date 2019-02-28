package org.broadinstitute.transporter.db

import cats.effect.IO
import doobie.util.log._
import io.chrisdavenport.log4cats.SelfAwareStructuredLogger

object DbLogHandler {

  def apply(logger: SelfAwareStructuredLogger[IO]): LogHandler = LogHandler {

    case Success(sql, args, dbTime, clientTime) =>
      logger.debug(s"""Successful statement execution:
           |
           |  ${sql.lines.dropWhile(_.trim.isEmpty).mkString("\n  ")}
           |
           |  args = [${args.mkString(",")}]
           |  exec = ${dbTime.toMillis} ms DB + ${clientTime.toMillis} ms client-side
        """.stripMargin).unsafeRunAsyncAndForget()

    case ExecFailure(sql, args, dbTime, err) =>
      logger.error(err)(s"""Statement execution failed:
           |
           |  ${sql.lines.dropWhile(_.trim.isEmpty).mkString("\n  ")}
           |
           |  args = [${args.mkString(",")}]
           |  exec = ${dbTime.toMillis} ms DB
         """.stripMargin).unsafeRunAsyncAndForget()

    case ProcessingFailure(sql, args, dbTime, clientTime, err) =>
      logger.error(err)(s"""Result-set processing failed:
         |
         |  ${sql.lines.dropWhile(_.trim.isEmpty).mkString("\n  ")}
         |
         |  args = [${args.mkString(",")}]
         |  exec = ${dbTime.toMillis} ms DB + ${clientTime.toMillis} ms client-side
       """.stripMargin).unsafeRunAsyncAndForget()
  }
}
