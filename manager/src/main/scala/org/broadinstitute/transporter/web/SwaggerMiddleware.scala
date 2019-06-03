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

  /** Top-level route which serves generated API docs. */
  private val apiDocsPath = "api-docs.json"

  /**
    * Top-level route which serves the Swagger UI.
    *
    * Our swagger routes change every time we bump the UI's version, so we
    * define stable aliases to expose in external systems / documentation.
    */
  private val stableUIPath = "swagger-ui"

  /**
    * Callback page for Google to redirect to on successful OAuth logins.
    *
    * Our swagger routes change every time we bump the UI's version, so we
    * define stable aliases to expose in external systems / documentation.
    */
  private val stableOAuthRedirectPath = "oauth2-redirect"

  /** Route which serves the Swagger UI. */
  private val apiUiPath =
    /*
     * Usually hard-coding a reference to `BuildInfo` makes things
     * more difficult to test, but in this case there's only ever
     * one safe choice for the swagger parameters so there's no point
     * in taking them as arguments.
     */
    s"/${BuildInfo.swaggerLibrary}/${BuildInfo.swaggerVersion}/index.html"

  /** Route which serves the OAuth redirect logic for the Swagger UI. */
  private val oauthRedirectPath =
    s"/${BuildInfo.swaggerLibrary}/${BuildInfo.swaggerVersion}/oauth2-redirect.html"

  /**
    * Information to display in the "Authorize" box in Swagger,
    * driving the authorization flow.
    */
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

    /*
     * Catch-all routes matching anything within the resources of the the Swagger webjar
     * _except for_ the index page, which we have to modify in-flight to inject app-specific
     * info.
     */
    val swaggerUiAssetRoutes = WebjarService[IO](
      config = Config(
        blockingEc,
        asset =>
          asset.library == BuildInfo.swaggerLibrary &&
            asset.version == BuildInfo.swaggerVersion &&
            asset.asset != "index.html"
      )
    )

    /*
     * Route matching only the Swagger index page.
     *
     * We inject two types of information by rewriting the HTML on its way to the client:
     *   1. We rewrite URLs to point at Transporter routes
     *   2. If OAuth config is given, we add a call to the 'initOAuth' method
     *
     * This seems to be standard practice for apps that serve Swagger from resources,
     * at least in DSP. For example, see CromIAM at:
     *
     * https://github.com/broadinstitute/cromwell/blob/master/CromIAM/src/main/scala/cromiam/webservice/SwaggerUiHttpService.scala
     */
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
            /*
             * Replace the dummy "petstore" URL with the route to this app's API docs.
             * Also replace the OAuth redirect URL here since it goes in the same method
             * call on the front-end, even if OAuth isn't going to be enabled.
             *
             * NOTE: We redirect to a stable alias instead of the versioned HTML page in the
             * resources jar because Google requires that allowable redirect URLs be white-listed
             * in the cloud console, and this is easier to maintain than it would be to add a new
             * white-listed URL every time we bump the Swagger UI's version.
             */
            """url: "https://petstore.swagger.io/v2/swagger.json",""",
            s"""url: "/$apiDocsPath",
               |validatorUrl: null,
               |oauth2RedirectUrl: window.location.origin + "/$stableOAuthRedirectPath",""".stripMargin
          )

          /*
           * If auth config is given, inject a call to initialize the OAuth dialog on
           * the front-end. The OAuth client ID will be visible in the page's source code,
           * but apparently that's Just How Swagger Works.
           */
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

    // Convenience routes for redirecting users to versioned Swagger routes.
    val swaggerUiRoutes = HttpRoutes.of[IO] {
      case GET -> Root =>
        TemporaryRedirect(Location(Uri.unsafeFromString(s"/$stableUIPath")))
      case GET -> Root / segment if segment == stableUIPath =>
        TemporaryRedirect(Location(Uri.unsafeFromString(apiUiPath)))
      case GET -> Root / segment if segment == stableOAuthRedirectPath =>
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
