package org.broadinstitute.transporter.api

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.broadinstitute.transporter.BuildInfo
import org.http4s.{HttpRoutes, Uri}
import org.http4s.dsl.io._
import org.http4s.headers.Location
import org.http4s.server.staticcontent.WebjarService
import org.http4s.server.staticcontent.WebjarService.Config

import scala.concurrent.ExecutionContext

class SwaggerUiApi(
  apiDocsPath: String,
  blockingEc: ExecutionContext
)(implicit cs: ContextShift[IO]) {

  private val swaggerLib = "swagger-ui"

  private val swaggerUiAssetRoutes = WebjarService[IO](
    config = Config(blockingEc, _.library == swaggerLib)
  )

  private val swaggerUiRoutes = HttpRoutes.of[IO] {
    case GET -> Root =>
      TemporaryRedirect(Location(Uri.uri("/api-docs")))
    case GET -> Root / "api-docs" =>
      TemporaryRedirect(
        Location(
          Uri.unsafeFromString(
            // Usually hard-coding a reference to `BuildInfo` makes things more difficult to test,
            // but in this case there's only ever one safe choice for the swagger version so there's
            // no point in parameterizing it.
            s"/$swaggerLib/${BuildInfo.swaggerVersion}/index.html?url=/$apiDocsPath"
          )
        )
      )
  }

  def routes: HttpRoutes[IO] =
    swaggerUiAssetRoutes.combineK(swaggerUiRoutes)
}
