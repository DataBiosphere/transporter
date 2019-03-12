package org.broadinstitute.transporter.web

import cats.effect.IO
import org.broadinstitute.transporter.info.{InfoController, ManagerStatus, ManagerVersion}
import org.http4s._
import org.http4s.rho.RhoRoutes

/**
  * Container for Transporter's "info" (unversioned, non-API) routes.
  *
  * NOTE: We use Rho to generate Swagger documentation from the definitions here.
  * The derivation can sometimes go haywire, but (so far) we've been able to find
  * workarounds.
  */
class InfoRoutes(infoController: InfoController) extends RhoRoutes[IO] {

  private val statusRoute = (GET / "status")
    .withDescription("Query operational status of the system")

  private val versionRoute = (GET / "version")
    .withDescription("Query version of the system")

  /*
   * NOTES on weirdness discovered in Rho that you'll see below:
   *
   *   1. Thunks are required on no-arg routes to prevent eager
   *      evaluation & result caching on app startup. This is
   *      desired behavior according to the Rho maintainers.
   *
   *   2. http4s provides a generic method for deriving `EntityEncoder`
   *      instances for any type with a circe `Encoder`, but for some
   *      reason combining it w/ the thunks causes divergence in
   *      implicit search. There are a couple maybe-related bugs filed
   *      in Rho; I'll also try to minimize & file a bug. Until then,
   *      we have to live with the boilerplate.
   */

  statusRoute |>> { () =>
    implicit val encoder: EntityEncoder[IO, ManagerStatus] =
      org.http4s.circe.jsonEncoderOf

    infoController.status.flatMap { status =>
      if (status.ok) {
        Ok(status)
      } else {
        InternalServerError(status)
      }
    }
  }

  versionRoute |>> { () =>
    implicit val encoder: EntityEncoder[IO, ManagerVersion] =
      org.http4s.circe.jsonEncoderOf

    infoController.version.flatMap(Ok(_))
  }
}
