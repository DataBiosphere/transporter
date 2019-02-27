package org.broadinstitute.transporter.web

import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.broadinstitute.transporter.BuildInfo
import org.http4s.dsl.io._
import org.http4s.headers.Location
import org.http4s.{HttpRoutes, Uri}
import org.http4s.rho.RhoRoutes
import org.http4s.rho.bits.PathAST.{PathMatch, TypedPath}
import org.http4s.rho.swagger.models.Info
import org.http4s.rho.swagger.syntax.{io => swaggerIo}
import org.http4s.server.staticcontent.WebjarService
import org.http4s.server.staticcontent.WebjarService.Config

import scala.concurrent.ExecutionContext

/**
  * Converter from `RhoRoutes[IO]` to `HttpRoutes[IO]` which
  * injects Swagger-generating and Swagger-serving routes into
  * the final set of routes.
  *
  * Effectively a function, but we follow the http4s pattern of
  * using an `object` + `apply` to avoid rough patches that can
  * come from extending the built-in `FunctionX` types.
  */
object SwaggerMiddleware {

  /** Top-level route which should serve generated API docs. */
  private val apiDocsPath = "api-docs.json"

  /** Top-level route which should serve the Swagger UI. */
  private val apiUiPath =
    /*
     * Usually hard-coding a reference to `BuildInfo` makes things
     * more difficult to test, but in this case there's only ever
     * one safe choice for the swagger parameters so there's no point
     * in taking them as arguments.
     */
    s"/${BuildInfo.swaggerLibrary}/${BuildInfo.swaggerVersion}/index.html?url=/$apiDocsPath"

  /**
    * Convert a set of Rho routes tagged with documentation into
    * a set of runnable http4s routes, injecting auto-generated
    * documentation routes into the final set.
    *
    * @param routesToDocument rho routes to convert
    * @param headerInfo app-level information to include as the header
    *                   of the generated API documentation
    * @param blockingEc execution context which should run the
    *                   blocking I/O needed to read Swagger assets
    *                   from application resources on requests
    * @param cs proof of the ability to shift IO-wrapped computations
    *           onto other threads
    */
  def apply(
    routesToDocument: NonEmptyList[RhoRoutes[IO]],
    headerInfo: Info,
    blockingEc: ExecutionContext
  )(
    implicit cs: ContextShift[IO]
  ): HttpRoutes[IO] = {
    // Catch-all routes matching anything within the resources of the the Swagger webjar.
    val swaggerUiAssetRoutes = WebjarService[IO](
      config = Config(
        blockingEc,
        asset =>
          asset.library == BuildInfo.swaggerLibrary &&
            asset.version == BuildInfo.swaggerVersion
      )
    )

    // Convenience routes for redirecting users to the UI's specific route.
    val swaggerUiRoutes = HttpRoutes.of[IO] {
      case GET -> Root =>
        TemporaryRedirect(Location(Uri.uri("/api-docs")))
      case GET -> Root / "api-docs" =>
        TemporaryRedirect(Location(Uri.unsafeFromString(apiUiPath)))
    }

    val swaggerMiddleware = swaggerIo.createRhoMiddleware(
      apiInfo = headerInfo,
      apiPath = TypedPath(PathMatch(apiDocsPath))
    )

    // Converts the Rho routes to corresponding http4s routes, then tacks
    // a route to serve generated API documentation onto the end.
    val documentedRoutes = routesToDocument
      .reduce[RhoRoutes[IO]](_.and(_))
      .toRoutes(swaggerMiddleware)

    // `combineK` is like `combine`, but for type functions.
    // Ex: `Int` has `combine` (+), but `List` has `combineK` (::)
    documentedRoutes
      .combineK(swaggerUiAssetRoutes)
      .combineK(swaggerUiRoutes)
  }
}
