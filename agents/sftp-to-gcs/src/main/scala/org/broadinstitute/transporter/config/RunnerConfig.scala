package org.broadinstitute.transporter.config

import java.nio.file.Path

import org.broadinstitute.monster.storage.sftp.SftpLoginInfo
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/** Top-level configuration for the SFTP->GCS Transporter agent. */
case class RunnerConfig(
  sftp: SftpLoginInfo,
  gcsServiceAccount: Option[Path],
  timeouts: TimeoutConfig,
  retries: RetryConfig,
  mibPerStep: Int,
  maxConcurrentReads: Int
)

object RunnerConfig {
  // Don't listen to IntelliJ; needed for deriving the SftpLoginInfo reader.
  import pureconfig.generic.auto._

  implicit val reader: ConfigReader[RunnerConfig] = deriveReader
}
