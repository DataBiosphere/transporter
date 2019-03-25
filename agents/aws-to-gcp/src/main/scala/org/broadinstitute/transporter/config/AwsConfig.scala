package org.broadinstitute.transporter.config

import java.time.Duration

import cats.effect.IO
import pureconfig.ConfigReader
import pureconfig.error.CannotConvert
import pureconfig.generic.semiauto.deriveReader
import software.amazon.awssdk.auth.credentials.{
  AwsBasicCredentials,
  StaticCredentialsProvider
}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.core.retry.RetryPolicy
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client

import scala.collection.JavaConverters._

/** Configuration determining how the AWS->GCP agent should connect to S3. */
case class AwsConfig(
  accessKeyId: String,
  secretAccessKey: String,
  region: Region
) {

  /** Build an S3 client using the auth configuration from this object. */
  def toClient: IO[S3Client] = IO.delay {
    import RetryConstants._

    val credentials = AwsBasicCredentials.create(accessKeyId, secretAccessKey)
    val provider = StaticCredentialsProvider.create(credentials)

    val overrideConfig = ClientOverrideConfiguration
      .builder()
      .retryPolicy(
        RetryPolicy
          .builder()
          .numRetries(RetryCount)
          .backoffStrategy(
            FullJitterBackoffStrategy
              .builder()
              .baseDelay(Duration.ofMillis(RetryInitDelay.toMillis))
              .maxBackoffTime(Duration.ofMillis(RetryMaxDelay.toMillis))
              .build()
          )
          .build()
      )
      .apiCallAttemptTimeout(Duration.ofMillis(SingleRequestTimeout.toMillis))
      .apiCallTimeout(Duration.ofMillis(TotalRequestTimeout.toMillis))
      .build()

    S3Client
      .builder()
      .region(region)
      .credentialsProvider(provider)
      .overrideConfiguration(overrideConfig)
      .build()
  }
}

object AwsConfig {

  /** Convenience converter from arbitrary strings into AWS Regions. */
  implicit val regionReader: ConfigReader[Region] =
    ConfigReader.stringConfigReader.emap { str =>
      Region
        .regions()
        .asScala
        .find(_.id() == str)
        .toRight(CannotConvert(str, "Region", "Not found in list of valid AWS regions"))
    }

  implicit val reader: ConfigReader[AwsConfig] = deriveReader
}
