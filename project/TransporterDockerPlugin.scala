import com.typesafe.sbt.packager.NativePackagerKeys
import com.typesafe.sbt.packager.archetypes.scripts.AshScriptPlugin
import com.typesafe.sbt.packager.docker.DockerPlugin
import com.typesafe.sbt.packager.linux.LinuxKeys
import sbt._
import sbt.Keys._
import sbt.plugins.JvmPlugin

object TransporterDockerPlugin extends AutoPlugin with LinuxKeys with NativePackagerKeys {
  import DockerPlugin.autoImport._

  override def requires: Plugins = JvmPlugin && DockerPlugin && AshScriptPlugin

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    dockerBaseImage := "openjdk:8",
    dockerRepository := Some("broadinstitute"),
    dockerExposedPorts := Seq(8080),
    dockerLabels := Map("TRANSPORTER_VERSION" -> version.value),
    Docker / defaultLinuxInstallLocation := "/app",
    Docker / maintainer := "monster@broadinstitute.org"
  )

}