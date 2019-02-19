package org.broadinstitute.transporter.api

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.http4s.{HttpRoutes, Uri}
import org.http4s.dsl.io._
import org.http4s.headers.Location
import org.http4s.server.staticcontent.WebjarService
import org.http4s.server.staticcontent.WebjarService.Config

import scala.concurrent.ExecutionContext

class SwaggerUiApi(
  swaggerVersion: String,
  apiDocsPath: String,
  blockingEc: ExecutionContext
)(implicit cs: ContextShift[IO]) {

  private val swaggerLib = "swagger-ui"

  private val swaggerUiAssetRoutes = WebjarService[IO](
    config = Config(blockingEc, _.library == swaggerLib)
  )

  private val swaggerUiRoute = HttpRoutes.of[IO] {
    case GET -> Root / "api-docs" =>
      Found(
        Location(
          Uri.unsafeFromString(
            s"/$swaggerLib/$swaggerVersion/index.html?url=/$apiDocsPath"
          )
        )
      )
  }

  def routes: HttpRoutes[IO] =
    swaggerUiAssetRoutes.combineK(swaggerUiRoute)
}
