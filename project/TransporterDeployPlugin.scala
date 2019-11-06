import com.typesafe.sbt.SbtNativePackager.autoImport.NativePackagerHelper
import com.typesafe.sbt.packager.NativePackagerKeys
import com.typesafe.sbt.packager.docker.{Cmd, DockerChmodType, DockerPlugin}
import com.typesafe.sbt.packager.linux.LinuxKeys
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._
import org.broadinstitute.monster.sbt.MonsterDockerPlugin
import sbt._
import sbt.Keys._

import sys.process.Process

/**
  * Docker plugin for transporter components, defines a variety of configurations
  * for init containers when enabled for a project.
  */
object TransporterDeployPlugin extends AutoPlugin with LinuxKeys with NativePackagerKeys {
  import DockerPlugin.autoImport._

  override def requires: Plugins = MonsterDockerPlugin

  object autoImport {

    val terraformStagingDirectory = settingKey[File](
      "Local directory where Terraform files will be staged before bundling"
    )

    val terraformSnapshotBucket = settingKey[String](
      "Name of the GCS bucket (without gs://) to which Terraform snapshots should be published"
    )

    val terraformReleaseBucket = settingKey[String](
      "Name of the GCS bucket (without gs://) to which Terraform releases should be published"
    )

    val terraformVars = taskKey[Map[String, String]](
      "Variables to replace in Terraform files before they are bundled"
    )
  }
  import autoImport._

  private def deployId(projectName: String) = s"$projectName-deploy"

  /**
    * Copy all files from `sourceDir` into `targetDir`, replacing defined
    * Terraform variables in the process.
    *
    * @return the list of rewritten files, paired with their paths relative
    *         to the top-level target directory
    */
  private def prepTerraformBundle(
    sourceDir: File,
    targetDir: File,
    vars: Map[String, String]
  ): Seq[(File, String)] = {
    val makeRelative = NativePackagerHelper.relativeTo(sourceDir)
    val sourceFiles = sourceDir.allPaths
    sourceFiles.get().filterNot(_.isDirectory).flatMap { source =>
      val withVars = vars.foldLeft(IO.read(source)) {
        case (acc, (key, value)) =>
          acc.replaceAll(s"<<$key>>", value)
      }
      makeRelative(source).fold(Seq.empty[(File, String)]) { targetPath =>
        val targetFile = targetDir / targetPath
        IO.write(targetFile, withVars)
        Seq(targetFile -> targetPath)
      }
    }
  }

  /**
    * The bucket to use when publishing Terraform bundles for the specific
    * version computed at publish time.
    */
  private lazy val terraformBucket = Def.settingDyn {
    if (isSnapshot.value) {
      terraformSnapshotBucket
    } else {
      terraformReleaseBucket
    }
  }

  override def derivedProjects(proj: ProjectDefinition[_]): List[Project] = {
    val deployProject = project
      .withId(deployId(proj.id))
      .in(new File(proj.base, "deploy"))
      .enablePlugins(DockerPlugin)
      .settings(
        dockerBaseImage := "broadinstitute/configurator-base:1.0.2",
        dockerRepository := Some("us.gcr.io/broad-dsp-gcr-public"),
        // Match DSP convention for init container names.
        Docker / packageName := s"${proj.id}-config",
        Docker / defaultLinuxInstallLocation := "/configs",
        Docker / maintainer := "monster@broadinstitute.org",
        Universal / mappings := (Compile / baseDirectory).map { baseDir =>
          val sourceDir = baseDir / "init-containers"
          val makeRelative = NativePackagerHelper.relativeTo(sourceDir)
          NativePackagerHelper.directory(sourceDir).flatMap {
            case (f, _) => makeRelative(f).map(relative => f -> relative)
          }
        }.value,
        dockerChmodType := DockerChmodType.UserGroupWriteExecute,
        dockerEntrypoint := Seq("/usr/local/bin/cp-config.sh"),
        dockerCommands := dockerCommands.value :+ Cmd("WORKDIR", "/working"),
        terraformStagingDirectory := (Compile / resourceManaged).value / "terraform",
        terraformSnapshotBucket := "broad-dsp-monster-terraform-snapshots",
        terraformReleaseBucket := "broad-dsp-monster-terraform-releases",
        terraformVars := Map("VERSION" -> version.value),
        publish := {
          // Publish init container to GCR.
          (Docker / publish).value

          // Bundle Terraform templates into a zip, and publish to GCS.
          val targetBucket = terraformBucket.value
          val source = baseDirectory.value / "terraform"
          val varsToReplace = terraformVars.value
          val staging = terraformStagingDirectory.value
          val tfFiles = prepTerraformBundle(source, staging, varsToReplace)

          val zipName = s"${proj.id}-v${version.value}.zip"
          val zipFile = staging / zipName

          IO.zip(tfFiles, zipFile)

          // TODO: Use a client library instead of assuming gsutil exists.
          val targetPath = s"gs://$targetBucket/${proj.id}/$zipName"
          val copyCode =
            Process(s"gsutil cp ${zipFile.getAbsolutePath} $targetPath").run().exitValue()
          if (copyCode != 0) {
            sys.error(
              s"Failed to upload Terraform bundle to GCS, got exit code: $copyCode"
            )
          }
        }
      )

    List(deployProject)
  }

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    publish := Def.taskDyn {
      // Run the existing publish behavior for this project.
      publish.value
      // Also publish the derived project.
      LocalProject(deployId(name.value)) / publish
    }.value
  )
}
