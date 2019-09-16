// Settings to apply across the entire build.
enablePlugins(GitVersioning)
inThisBuild(
  Seq(
    organization := "org.broadinstitute",
    scalaVersion := "2.12.8",
    // Auto-format
    scalafmtConfig := (ThisBuild / baseDirectory)(_ / ".scalafmt.conf").value,
    scalafmtOnCompile := true,
    // Recommended guardrails
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding",
      "UTF-8",
      "-explaintypes",
      "-feature",
      "-target:jvm-1.8",
      "-unchecked",
      "-Xcheckinit",
      "-Xfatal-warnings",
      "-Xfuture",
      "-Xlint",
      "-Xmax-classfile-name",
      "200",
      "-Yno-adapted-args",
      "-Ypartial-unification",
      "-Ywarn-dead-code",
      "-Ywarn-extra-implicit",
      "-Ywarn-inaccessible",
      "-Ywarn-infer-any",
      "-Ywarn-nullary-override",
      "-Ywarn-nullary-unit",
      "-Ywarn-numeric-widen",
      "-Ywarn-unused",
      "-Ywarn-value-discard"
    )
  )
)

// Compiler plugins.
val betterMonadicForVersion = "0.3.1"

// Configuration.
val pureConfigVersion = "0.11.1"

// DB.
val doobieVersion = "0.7.0"
val postgresqlDriverVersion = "42.2.5"

// JSON.
val circeVersion = "0.11.1"
val circeDerivationVersion = "0.11.0-M1"
val enumeratumCirceVersion = "1.5.21"
val everitJsonSchemaVersion = "1.11.1"

// Kafka.
val fs2KafkaVersion = "0.19.9"
val kafkaVersion = "2.2.1"

// Logging.
val logbackVersion = "1.2.3"
val log4catsVersion = "0.3.0"
val log4sVersion = "1.8.2"

// Transfer.
val googleAuthVersion = "0.16.2"

// Utils.
val enumeratumVersion = "1.5.13"

// Web.
val storageLibsVersion = "0.4.0"
val http4sVersion = "0.20.10"
val swaggerUiModule = "swagger-ui"
val swaggerUiVersion = "3.23.5"
val tapirVersion = "0.9.3"

// Testing.
val liquibaseVersion = "3.7.0"
val scalaMockVersion = "4.2.0"
val scalaTestVersion = "3.0.8"
val testcontainersVersion = "1.12.0"
val testcontainersScalaVersion = "0.29.0"

// Settings to apply to all sub-projects.
// Can't be applied at the build level because of scoping rules.
val commonSettings = Seq(
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % betterMonadicForVersion),
  resolvers ++= Seq(
    "Broad Artifactory Releases" at "https://broadinstitute.jfrog.io/broadinstitute/libs-release/",
    "Broad Artifactory Snapshots" at "https://broadinstitute.jfrog.io/broadinstitute/libs-snapshot/"
  ),
  Compile / console / scalacOptions := (Compile / scalacOptions).value.filterNot(
    Set(
      "-Xfatal-warnings",
      "-Xlint",
      "-Ywarn-unused",
      "-Ywarn-unused-import"
    )
  ),
  Compile / doc / scalacOptions += "-no-link-warnings",
  Test / fork := true,
  Test / javaOptions ++= Seq(
    // Limit threads in global pool to make sure we're not
    // accidentally avoiding deadlocks on our laptops.
    "-Dscala.concurrent.context.numThreads=1",
    "-Dscala.concurrent.context.maxThreads=1",
    "-Dscala.concurrent.context.maxExtraThreads=0"
  )
)

val commonNoPublish = commonSettings :+ (publish / skip := true)

lazy val transporter = project
  .in(file("."))
  .settings(publish / skip := true)
  .aggregate(
    `transporter-common`,
    `transporter-manager`,
    `transporter-agent-template`,
    `transporter-aws-to-gcp-agent`,
    `transporter-gcs-to-gcs-agent`
  )

/** Definitions used by both the manager and agents. */
lazy val `transporter-common` = project
  .in(file("./common"))
  .settings(commonNoPublish)
  .settings(
    // Needed to resolve JSON schema lib.
    resolvers += "Jitpack" at "https://jitpack.io",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.beachape" %% "enumeratum" % enumeratumVersion,
      "com.beachape" %% "enumeratum-circe" % enumeratumCirceVersion,
      "com.github.everit-org.json-schema" % "org.everit.json.schema" % everitJsonSchemaVersion,
      "com.github.pureconfig" %% "pureconfig" % pureConfigVersion,
      "com.github.pureconfig" %% "pureconfig-cats" % pureConfigVersion,
      "com.ovoenergy" %% "fs2-kafka" % fs2KafkaVersion,
      "io.chrisdavenport" %% "log4cats-slf4j" % log4catsVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-derivation" % circeDerivationVersion,
      "io.circe" %% "circe-parser" % circeVersion
    ),
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-literal" % circeVersion,
      "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % Test),
    dependencyOverrides ++= Seq(
      "org.apache.kafka" % "kafka-clients" % kafkaVersion
    )
  )

/**
  * Database migrations needed by the manager.
  *
  * Pulled into a separate project to make it easy to package them into a
  * liquibase Docker image.
  */
lazy val `transporter-manager-migrations` = project
  .in(file("./manager/db-migrations"))
  .enablePlugins(TransporterDockerPlugin)
  .settings(commonSettings)
  .settings(
    // Rewrite the Docker mappings to only include changelog files stored at /working.
    // Relative structure of the changelogs is preserved.
    Universal / mappings := (Compile / resourceDirectory).map { resourceDir =>
      val makeRelative = NativePackagerHelper.relativeTo(resourceDir)
      NativePackagerHelper.directory(resourceDir).flatMap {
        case (f, _) => makeRelative(f).map(relative => f -> s"/db/$relative")
      }
    }.value,
    // Build off an existing "liquibase-postgres" Docker image.
    dockerCommands := {
      import com.typesafe.sbt.packager.docker._

      Seq(
        Cmd("FROM", "kilna/liquibase-postgres"),
        Cmd("LABEL", s"MAINTAINER=${(Docker / maintainer).value}"),
        Cmd("LABEL", s"TRANSPORTER_VERSION=${version.value}"),
        Cmd("COPY", "app/db /workspace")
      )
    },
    // Override 'run' to migrate a local database, for easy manual testing.
    Compile / run := {
      // Build the migration Docker image locally before running.
      (Docker / publishLocal).value

      import scala.sys.process._

      val image = dockerAlias.value.toString()

      val envVars = Map(
        // NOTE: Only works on Docker for OS X.
        // Assumes default Postgres install from Homebrew.
        "LIQUIBASE_HOST" -> "host.docker.internal",
        "LIQUIBASE_DATABASE" -> "postgres",
        "LIQUIBASE_USERNAME" -> "whoami".!!
      )

      Seq
        .concat(
          Seq("docker", "run", "--rm"),
          envVars.flatMap { case (k, v) => Seq("-e", s"$k=$v") },
          Seq(image, "liquibase", "update")
        )
        .!!

    }
  )

/** Web service which receives, distributes, and tracks transfer requests. */
lazy val `transporter-manager` = project
  .in(file("./manager"))
  .enablePlugins(BuildInfoPlugin, TransporterDeployPlugin)
  .dependsOn(`transporter-common`, `transporter-manager-migrations` % Test)
  .settings(commonSettings)
  .settings(
    publish := {
      publish.value
      (`transporter-manager-migrations` / publish).value
    },
    libraryDependencies ++= Seq(
      "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion,
      "com.softwaremill.tapir" %% "tapir-core" % tapirVersion,
      "com.softwaremill.tapir" %% "tapir-json-circe" % tapirVersion,
      "com.softwaremill.tapir" %% "tapir-http4s-server" % tapirVersion,
      "com.softwaremill.tapir" %% "tapir-openapi-docs" % tapirVersion,
      "com.softwaremill.tapir" %% "tapir-openapi-circe-yaml" % tapirVersion,
      "org.http4s" %% "http4s-blaze-server" % http4sVersion,
      "org.http4s" %% "http4s-circe" % http4sVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.tpolecat" %% "doobie-core" % doobieVersion,
      "org.tpolecat" %% "doobie-hikari" % doobieVersion,
      "org.tpolecat" %% "doobie-postgres" % doobieVersion,
      "org.tpolecat" %% "doobie-postgres-circe" % doobieVersion,
      "org.webjars" % swaggerUiModule % swaggerUiVersion
    ),
    libraryDependencies ++= Seq(
      "com.dimafeng" %% "testcontainers-scala" % testcontainersScalaVersion,
      "io.circe" %% "circe-literal" % circeVersion,
      "io.github.embeddedkafka" %% "embedded-kafka" % kafkaVersion,
      "org.liquibase" % "liquibase-core" % liquibaseVersion,
      "org.scalamock" %% "scalamock" % scalaMockVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion,
      "org.testcontainers" % "postgresql" % testcontainersVersion
    ).map(_ % Test),
    dependencyOverrides := Seq(
      "org.postgresql" % "postgresql" % postgresqlDriverVersion,
      "org.testcontainers" % "testcontainers" % testcontainersVersion % Test
    ),
    // Inject version information into the app.
    buildInfoKeys := Seq(
      version,
      "swaggerVersion" -> swaggerUiVersion,
      "swaggerLibrary" -> swaggerUiModule
    ),
    buildInfoPackage := "org.broadinstitute.transporter"
  )

/** Common framework for transfer-executing agents. */
lazy val `transporter-agent-template` = project
  .in(file("./agents/template"))
  .dependsOn(`transporter-common`)
  .settings(commonNoPublish)
  .settings(
    libraryDependencies ++= Seq(
      "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion,
      "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
      "org.http4s" %% "http4s-blaze-server" % http4sVersion,
      "org.http4s" %% "http4s-circe" % http4sVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.log4s" %% "log4s" % log4sVersion
    ),
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-literal" % circeVersion,
      "org.scalamock" %% "scalamock" % scalaMockVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion,
      // NOTE: jakarta.ws.rs-api is the same project as javax.ws.rs-api, they just changed their
      // organization ID. Something in how the change happend runs across a bug in sbt / coursier,
      // so we have to manually exclude the old name and pull in the new name.
      // See: https://github.com/sbt/sbt/issues/3618
      "io.github.embeddedkafka" %% "embedded-kafka-streams" % kafkaVersion exclude ("javax.ws.rs", "javax.ws.rs-api"),
      "jakarta.ws.rs" % "jakarta.ws.rs-api" % "2.1.5"
    ).map(_ % Test)
  )

/** Agent which can transfer files from AWS to GCP. */
lazy val `transporter-aws-to-gcp-agent` = project
  .in(file("./agents/aws-to-gcp"))
  .enablePlugins(TransporterDeployPlugin)
  .dependsOn(`transporter-agent-template`)
  .settings(commonSettings)
  .settings(
    resolvers += Resolver.jcenterRepo,
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-blaze-client" % http4sVersion,
      "com.google.auth" % "google-auth-library-oauth2-http" % googleAuthVersion
    ),
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % Test)
  )

lazy val `transporter-gcs-to-gcs-agent` = project
  .in(file("./agents/gcs-to-gcs"))
  .enablePlugins(TransporterDeployPlugin)
  .dependsOn(`transporter-agent-template`)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.broadinstitute.monster" %% "gcs-lib" % storageLibsVersion
    ),
    libraryDependencies ++= Seq(
      "org.scalamock" %% "scalamock" % scalaMockVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % Test)
  )
