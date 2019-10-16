import com.typesafe.sbt.packager.NativePackagerKeys
import com.typesafe.sbt.packager.archetypes.scripts.AshScriptPlugin
import com.typesafe.sbt.packager.docker.DockerPlugin
import com.typesafe.sbt.packager.linux.LinuxKeys
import org.broadinstitute.monster.sbt.BasePlugin
import sbt._
import sbt.Keys._

/**
  * Docker plugin for transporter components, defines a variety of configurations
  * for app containers when enabled for a project.
  */
object TransporterDockerPlugin extends AutoPlugin with LinuxKeys with NativePackagerKeys {
  import DockerPlugin.autoImport._

  override def requires: Plugins = DockerPlugin && AshScriptPlugin && BasePlugin

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    dockerBaseImage := "openjdk:8",
    dockerRepository := Some("us.gcr.io/broad-dsp-gcr-public"),
    dockerExposedPorts := Seq(8080),
    dockerLabels := Map("TRANSPORTER_VERSION" -> version.value),
    Docker / defaultLinuxInstallLocation := "/app",
    Docker / maintainer := "monster@broadinstitute.org",
    // Make our CI life easier and set up publish delegation here.
    publish := (Docker / publish).value
  )

}
