package org.broadinstitute.transporter

import cats.effect.{Blocker, IO, Resource}
import org.broadinstitute.transporter.api.{
  SftpToGcsOutput => Out,
  SftpToGcsProgress => Progress,
  SftpToGcsRequest => In
}
import org.broadinstitute.transporter.config.RunnerConfig
import org.broadinstitute.transporter.transfer.{SftpToGcsRunner, TransferRunner}

/** Transporter agent which can copy files from an SFTP site into GCS. */
object SftpToGcsAgent extends TransporterAgent[RunnerConfig, In, Progress, Out] {

  override def runnerResource(
    config: RunnerConfig
  ): Resource[IO, TransferRunner[In, Progress, Out]] =
    for {
      blocker <- Blocker[IO]
      runner <- SftpToGcsRunner.resource(config, executionContext, blocker)
    } yield {
      runner
    }
}
