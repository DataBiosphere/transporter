package org.broadinstitute.transporter.transfer.auth

import java.nio.file.Files
import java.time.Instant

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, IO}
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import org.broadinstitute.transporter.config.GcpConfig
import org.http4s.{AuthScheme, Credentials, Request}
import org.http4s.headers.Authorization

/**
  * Utility which can add authorization info for Google Cloud to outgoing HTTP requests.
  *
  * @param googleCreds google creds client, pre-loaded to point at either default credentials
  *                    or service account credentials
  * @param credsLock lock for refreshing the raw google creds
  */
class GcsAuthProvider private[transfer] (
  googleCreds: GoogleCredentials,
  credsLock: Semaphore[IO]
) {

  /** Add authorization for GCP to an HTTP request. */
  def addAuth(request: Request[IO]): IO[Request[IO]] =
    accessToken.map { bearerToken =>
      val creds = Credentials.Token(AuthScheme.Bearer, bearerToken)
      request.transformHeaders(_.put(Authorization(creds)))
    }

  /**
    * Get an access token from the base Google credentials.
    *
    * The token will be refreshed if expired.
    */
  private def accessToken: IO[String] =
    for {
      now <- IO.delay(Instant.now())
      // Lock around the refresh check to prevent double-refreshes.
      _ <- credsLock.acquire.bracket(_ => maybeRefreshToken(now))(_ => credsLock.release)
      accessToken <- IO.delay(googleCreds.getAccessToken)
    } yield {
      accessToken.getTokenValue
    }

  /**
    * Check if the access token for the base Google credentials has expired,
    * and refresh it if so.
    */
  private def maybeRefreshToken(now: Instant): IO[Unit] = {
    val maybeExpirationInstant = for {
      token <- Option(googleCreds.getAccessToken)
      expiration <- Option(token.getExpirationTime)
    } yield {
      expiration.toInstant
    }

    /*
     * Token is valid if:
     *   1. It's not null, and
     *   2. Its expiration time hasn't passed
     *
     * We pretend the token expires a second early to give some wiggle-room.
     */
    val tokenIsValid = maybeExpirationInstant.exists(_.minusSeconds(1).isAfter(now))

    if (tokenIsValid) {
      IO.unit
    } else {
      IO.delay(googleCreds.refresh())
    }
  }
}

object GcsAuthProvider {

  /** Construct a GCP auth provider wrapping underlying Google credentials. */
  def build(config: GcpConfig)(implicit c: Concurrent[IO]): IO[GcsAuthProvider] =
    for {
      baseCreds <- IO.delay {
        config.serviceAccountJson.fold(GoogleCredentials.getApplicationDefault) {
          jsonPath =>
            ServiceAccountCredentials.fromStream(Files.newInputStream(jsonPath))
        }
      }
      scopedCreds <- IO.delay {
        /*
         * Scopes are GCP's legacy, pre-IAM method of controlling service account access
         * to resources. Setting the scope to "cloud-platform" effectively makes the scope
         * check a no-op, allowing us to toggle permissions purely through IAM.
         */
        baseCreds.createScoped("https://www.googleapis.com/auth/cloud-platform")
      }
      lock <- Semaphore[IO](1L)
    } yield {
      new GcsAuthProvider(scopedCreds, lock)
    }
}
