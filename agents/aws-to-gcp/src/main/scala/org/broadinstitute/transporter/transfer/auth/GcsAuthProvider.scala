package org.broadinstitute.transporter.transfer.auth

import java.nio.file.Files
import java.time.Instant

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, IO}
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import org.broadinstitute.transporter.config.GcpConfig
import org.http4s.{AuthScheme, Credentials, Request}
import org.http4s.headers.Authorization

class GcsAuthProvider private[transfer] (
  googleCreds: GoogleCredentials,
  credsLock: Semaphore[IO]
) {

  def addAuth(request: Request[IO]): IO[Request[IO]] =
    accessToken.map { bearerToken =>
      val creds = Credentials.Token(AuthScheme.Bearer, bearerToken)
      request.transformHeaders(_.put(Authorization(creds)))
    }

  private def accessToken: IO[String] =
    for {
      now <- IO.delay(Instant.now())
      _ <- credsLock.acquire.bracket(_ => maybeRefreshToken(now))(_ => credsLock.release)
      accessToken <- IO.delay(googleCreds.getAccessToken)
    } yield {
      accessToken.getTokenValue
    }

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

  def build(config: GcpConfig)(implicit c: Concurrent[IO]): IO[GcsAuthProvider] =
    for {
      baseCreds <- IO.delay {
        config.serviceAccountJson.fold(GoogleCredentials.getApplicationDefault) {
          jsonPath =>
            ServiceAccountCredentials.fromStream(Files.newInputStream(jsonPath))
        }
      }
      scopedCreds <- IO.delay {
        baseCreds.createScoped("https://www.googleapis.com/auth/cloud-platform")
      }
      lock <- Semaphore[IO](1L)
    } yield {
      new GcsAuthProvider(scopedCreds, lock)
    }
}
