package org.broadinstitute.transporter.api

import cats.effect.{ContextShift, Effect, IO}
import cats.implicits._
import io.circe.syntax._
import org.broadinstitute.transporter.ManagerApp
import org.http4s.circe.CirceEntityEncoder
import org.http4s.{Http, HttpRoutes, Uri}
import org.http4s.headers.Location
import org.http4s.implicits._
import org.http4s.rho.RhoRoutes
import org.http4s.rho.bits.PathAST.{PathMatch, TypedPath}
import org.http4s.rho.swagger.models.Info
import org.http4s.rho.swagger.syntax.{io => swaggerIo}
import org.http4s.server.staticcontent.WebjarService
import org.http4s.server.staticcontent.WebjarService.Config

import scala.concurrent.ExecutionContext

class ManagerApi(
  appVersion: String,
  swaggerVersion: String,
  app: ManagerApp,
  blockingEc: ExecutionContext
)(
  implicit eff: Effect[IO],
  cs: ContextShift[IO]
) {

  import swaggerIo._

  private val infoRoutes = new RhoRoutes[IO] with CirceEntityEncoder {

    "Query operational status of the system" **
      GET / "status" |>> app.statusController.status.map { status =>
      if (status.ok) {
        Ok(status.asJson)
      } else {
        InternalServerError(status.asJson)
      }
    }

    "Query version of the system" **
      GET / "version" |>> Ok(appVersion)
  }

  private val swaggerUiAssetRoutes =
    WebjarService[IO](
      config = Config(blockingEc, _.library == "swagger-ui")
    )

  private val swaggerUiRoute = {
    import org.http4s.dsl.io._

    HttpRoutes.of[IO] {
      case GET -> Root / "api-docs" =>
        Found(
          Location(
            Uri.unsafeFromString(
              s"/swagger-ui/$swaggerVersion/index.html?url=/api-docs.json"
            )
          )
        )
    }
  }

  def routes: Http[IO, IO] = {
    val swaggerMiddleware = swaggerIo.createRhoMiddleware(
      apiInfo = Info(
        title = "Transporter API",
        version = appVersion,
        description = Some("Bulk file-transfer system for data ingest / delivery.")
      ),
      apiPath = TypedPath(PathMatch("api-docs.json"))
    )

    infoRoutes
      .toRoutes(swaggerMiddleware)
      .combineK(swaggerUiAssetRoutes)
      .combineK(swaggerUiRoute)
      .orNotFound
  }
}
