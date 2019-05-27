package org.broadinstitute.transporter.web

import cats.effect.{ContextShift, IO}
import cats.implicits._
import io.circe.Json
import org.broadinstitute.transporter.BuildInfo
import org.broadinstitute.transporter.queue.QueueSchema
import org.broadinstitute.transporter.web.config.OAuthConfig
import org.http4s.dsl.io._
import org.http4s.headers.Location
import org.http4s._
import org.http4s.rho.RhoRoutes
import org.http4s.rho.bits.PathAST.{PathMatch, TypedPath}
import org.http4s.rho.swagger._
import org.http4s.rho.swagger.models.{
  AbstractProperty,
  Info,
  OAuth2VendorExtensionsDefinition,
  SecuritySchemeDefinition
}
import org.http4s.rho.swagger.syntax.{io => swaggerIo}
import org.http4s.server.staticcontent.WebjarService
import org.http4s.server.staticcontent.WebjarService.Config

import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe.typeOf

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
    s"/${BuildInfo.swaggerLibrary}/${BuildInfo.swaggerVersion}/index.html"

  private val oauthRedirectPath =
    s"/${BuildInfo.swaggerLibrary}/${BuildInfo.swaggerVersion}/oauth2-redirect.html"

  private val googleAuthDefinition = OAuth2VendorExtensionsDefinition(
    authorizationUrl = "https://accounts.google.com/o/oauth2/auth",
    vendorExtensions = Map.empty,
    flow = "implicit",
    scopes = OAuthConfig.AuthScopes.map(scope => scope -> s"$scope authorization").toMap
  )

  /**
    * Convert a set of Rho routes tagged with documentation into
    * a set of runnable http4s routes, injecting auto-generated
    * documentation routes into the final set.
    *
    * @param headerInfo app-level information to include as the header
    *                   of the generated API documentation
    * @param blockingEc execution context which should run the
    *                   blocking I/O needed to read Swagger assets
    *                   from application resources on requests
    * @param cs proof of the ability to shift IO-wrapped computations
    *           onto other threads
    */
  def apply(
    headerInfo: Info,
    unauthedRoutes: RhoRoutes[IO],
    apiRoutes: RhoRoutes[IO],
    googleAuthConfig: Option[OAuthConfig],
    blockingEc: ExecutionContext
  )(
    implicit cs: ContextShift[IO]
  ): HttpRoutes[IO] = {

    val addAuth = googleAuthConfig.isDefined
    val securityDefinitions = if (addAuth) {
      Map(OAuthConfig.AuthName -> googleAuthDefinition)
    } else {
      Map.empty[String, SecuritySchemeDefinition]
    }

    // Catch-all routes matching anything within the resources of the the Swagger webjar
    // *except for* the index page, which we have to modify in-flight to inject OAuth secrets.
    val swaggerUiAssetRoutes = WebjarService[IO](
      config = Config(
        blockingEc,
        asset =>
          asset.library == BuildInfo.swaggerLibrary &&
            asset.version == BuildInfo.swaggerVersion &&
            asset.asset != "index.html"
      )
    )

    val swaggerUiIndexRoutes = WebjarService[IO](
      config = Config(
        blockingEc,
        asset =>
          asset.library == BuildInfo.swaggerLibrary &&
            asset.version == BuildInfo.swaggerVersion &&
            asset.asset == "index.html"
      )
    ).map { response =>
      if (response.status.isSuccess) {
        val newBody = fs2.text.lines(response.bodyAsText).map { line =>
          val withUrls = line.replace(
            """url: "https://petstore.swagger.io/v2/swagger.json",""",
            s"""url: "/$apiDocsPath",
               |validatorUrl: null,
               |oauth2RedirectUrl: window.location.origin + "/oauth2-redirect",""".stripMargin
          )
          googleAuthConfig.fold(withUrls) { config =>
            withUrls.replace(
              "window.ui = ui",
              s"""ui.initOAuth({
                 |  appName: "Swagger Auth",
                 |  clientId: "${config.clientId}",
                 |  scopeSeparator: " "
                 |})
                 |window.ui = ui""".stripMargin
            )
          }
        }

        response
          .withEntity(newBody.intersperse("\n"))
          .withContentTypeOption(response.contentType)
      } else {
        response
      }
    }

    // Convenience routes for redirecting users to the UI's specific route.
    val swaggerUiRoutes = HttpRoutes.of[IO] {
      case GET -> Root =>
        TemporaryRedirect(Location(uri"/api-docs"))
      case GET -> Root / "api-docs" =>
        TemporaryRedirect(Location(Uri.unsafeFromString(apiUiPath)))
      case GET -> Root / "oauth2-redirect" =>
        TemporaryRedirect(Location(Uri.unsafeFromString(oauthRedirectPath)))
    }

    val swaggerMiddleware = swaggerIo.createRhoMiddleware(
      apiInfo = headerInfo,
      apiPath = TypedPath(PathMatch(apiDocsPath)),
      basePath = Some("/"),
      produces = List("application/json"),
      securityDefinitions = securityDefinitions,
      swaggerFormats = DefaultSwaggerFormats
        .withFieldSerializers(typeOf[QueueSchema], AbstractProperty(`type` = "object"))
        .withFieldSerializers(typeOf[Json], AbstractProperty(`type` = "object"))
    )

    // Converts the Rho routes to corresponding http4s routes, then tacks
    // a route to serve generated API documentation onto the end.
    val documentedRoutes = unauthedRoutes.and(apiRoutes).toRoutes(swaggerMiddleware)

    // `combineK` is like `combine`, but for type functions.
    // Ex: `Int` has `combine` (+), but `List` has `combineK` (::)
    documentedRoutes
      .combineK(swaggerUiIndexRoutes)
      .combineK(swaggerUiAssetRoutes)
      .combineK(swaggerUiRoutes)
  }
}
