package org.broadinstitute.transporter.api

import cats.effect.IO
import org.broadinstitute.transporter.ManagerApp
import org.broadinstitute.transporter.status.ManagerStatus
import org.http4s._
import org.http4s.rho.RhoRoutes
import org.http4s.rho.bits.PathAST.{PathMatch, TypedPath}
import org.http4s.rho.swagger.models.Info
import org.http4s.rho.swagger.SwaggerSupport

class ManagerApi(appVersion: String, apiDocsPath: String, app: ManagerApp)
    extends SwaggerSupport[IO] {

  // This is in-sourced from `CirceEntityEncoder`, with pinned type params.
  // It _should_ work to instead extend than trait, but for some reason that
  // gives diverging implicits errors.
  private implicit val statusEncoder: EntityEncoder[IO, ManagerStatus] =
    org.http4s.circe.jsonEncoderOf

  private val infoRoutes: RhoRoutes[IO] = new RhoRoutes[IO] {

    private val statusRoute =
      "Query operational status of the system" ** GET / "status"

    private val versionRoute =
      "Query version of the system" ** GET / "version"

    // NOTE: Has to be a thunk here to prevent eager evaluation / caching.
    statusRoute |>> { () =>
      app.statusController.status.flatMap { status =>
        if (status.ok) {
          Ok(status)
        } else {
          InternalServerError(status)
        }
      }
    }

    // NOTE: The response here is evaluated at startup and cached,
    // since it's not a thunk.
    versionRoute |>> Ok(appVersion)
  }

  def routes: HttpRoutes[IO] = {
    val swaggerMiddleware = createRhoMiddleware(
      apiInfo = Info(
        title = "Transporter API",
        version = appVersion,
        description = Some("Bulk file-transfer system for data ingest / delivery.")
      ),
      apiPath = TypedPath(PathMatch(apiDocsPath))
    )

    infoRoutes.toRoutes(swaggerMiddleware)
  }
}
