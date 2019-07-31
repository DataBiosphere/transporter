import com.typesafe.sbt.SbtNativePackager.autoImport.NativePackagerHelper
import com.typesafe.sbt.packager.NativePackagerKeys
import com.typesafe.sbt.packager.docker.{Cmd, DockerChmodType, DockerPlugin}
import com.typesafe.sbt.packager.linux.LinuxKeys
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport._
import sbt._
import sbt.Keys._

/**
  * Docker plugin for transporter components, defines a variety of configurations
  * for init containers when enabled for a project.
  */
object TransporterDeployPlugin extends AutoPlugin with LinuxKeys with NativePackagerKeys {
  import DockerPlugin.autoImport._

  override def requires: Plugins = TransporterDockerPlugin

  override def derivedProjects(proj: ProjectDefinition[_]): List[Project] = {
    val deployProject = project
      .withId(s"${proj.id}-deploy")
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
        dockerCommands := dockerCommands.value :+ Cmd("WORKDIR", "/working")
      )

    List(deployProject)
  }

}
