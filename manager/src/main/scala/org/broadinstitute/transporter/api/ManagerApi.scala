package org.broadinstitute.transporter.api

import cats.effect.{ContextShift, Effect, IO}
import cats.implicits._
import org.http4s.Http
import org.http4s.implicits._
import org.http4s.rho.RhoRoutes
import org.http4s.rho.swagger.models.Info
import org.http4s.rho.swagger.syntax.{io => swaggerIo}
import org.http4s.server.staticcontent.WebjarService
import org.http4s.server.staticcontent.WebjarService.Config

import scala.concurrent.ExecutionContext

class ManagerApi(
  appVersion: String /*, swaggerVersion: String*/,
  blockingEc: ExecutionContext
)(
  implicit eff: Effect[IO],
  cs: ContextShift[IO]
) {

  import swaggerIo._

  private val infoRoutes = new RhoRoutes[IO] {

    "Query operational status of the system" **
      GET / "status" |>> Ok("It's alive")

    "Query version of the system" **
      GET / "version" |>> Ok(appVersion)
  }

  private val swaggerUiRoutes =
    WebjarService[IO](
      config = Config(blockingEc, _.library == "swagger-ui")
    )

  def routes: Http[IO, IO] = {
    val swaggerMiddleware = swaggerIo.createRhoMiddleware(
      apiInfo = Info(title = "Transporter API", version = appVersion)
    )

    val appRoutes = infoRoutes.toRoutes(swaggerMiddleware)

    appRoutes.combineK(swaggerUiRoutes).orNotFound
  }
}
