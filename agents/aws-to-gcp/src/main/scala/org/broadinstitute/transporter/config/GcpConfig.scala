package org.broadinstitute.transporter.config

import java.nio.file.{Files, Path}

import cats.effect.IO
import com.google.api.gax.retrying.RetrySettings
import com.google.auth.Credentials
import com.google.auth.oauth2.{GoogleCredentials, ServiceAccountCredentials}
import com.google.cloud.http.HttpTransportOptions
import com.google.cloud.storage.{Storage, StorageOptions}
import org.threeten.bp.Duration
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/** Configuration determining how the AWS->GCP agent should connect to GCS. */
case class GcpConfig(serviceAccountJson: Option[Path]) {

  /** Build a GCS client using the auth configuration from this object. */
  def toClient: IO[Storage] = IO.delay {
    import RetryConstants._

    val credentials: Credentials =
      serviceAccountJson.fold(GoogleCredentials.getApplicationDefault) { jsonPath =>
        ServiceAccountCredentials.fromStream(Files.newInputStream(jsonPath))
      }

    StorageOptions
      .newBuilder()
      .setCredentials(credentials)
      .setTransportOptions(
        HttpTransportOptions.newBuilder
          .setConnectTimeout(120000)
          .setReadTimeout(120000)
          .build
      )
      .setRetrySettings(
        RetrySettings.newBuilder
          .setMaxAttempts(RetryCount)
          .setInitialRetryDelay(Duration.ofMillis(RetryInitDelay.toMillis))
          .setMaxRetryDelay(Duration.ofMillis(RetryMaxDelay.toMillis))
          .setRetryDelayMultiplier(2.0)
          .setInitialRpcTimeout(Duration.ofMillis(SingleRequestTimeout.toMillis))
          .setMaxRpcTimeout(Duration.ofMillis(SingleRequestTimeout.toMillis))
          .setRpcTimeoutMultiplier(1.0)
          .setTotalTimeout(Duration.ofMillis(TotalRequestTimeout.toMillis))
          .build
      )
      .build()
      .getService
  }
}

object GcpConfig {
  implicit val reader: ConfigReader[GcpConfig] = deriveReader
}
