// Configuration.
val pureConfigVersion = "0.12.1"

// DB.
val doobieVersion = "0.8.4"
val postgresqlDriverVersion = "42.2.5"

// JSON.
val circeVersion = "0.12.3"
val circeDerivationVersion = "0.12.0-M7"
val enumeratumCirceVersion = "1.5.22"
val everitJsonSchemaVersion = "1.12.0"

// Kafka.
val fs2KafkaVersion = "0.20.1"
val kafkaVersion = "2.3.0"

// Logging.
val logbackVersion = "1.2.3"
val log4catsVersion = "1.0.0"
val log4sVersion = "1.8.2"

// Transfer.
val googleAuthVersion = "0.18.0"

// Utils.
val enumeratumVersion = "1.5.13"

// Web.
val storageLibsVersion = "0.6.0"
val http4sVersion = "0.21.0-M5"
val swaggerUiModule = "swagger-ui"
val swaggerUiVersion = "3.24.0"
val tapirVersion = "0.11.6"

// Testing.
val liquibaseVersion = "3.8.0"
val scalaMockVersion = "4.4.0"
val scalaTestVersion = "3.0.8"
val testcontainersVersion = "1.12.2"
val testcontainersScalaVersion = "0.33.0"

lazy val transporter = project
  .in(file("."))
  .settings(publish / skip := true)
  .aggregate(
    `transporter-common`,
    `transporter-manager`,
    `transporter-agent-template`,
    `transporter-aws-to-gcp-agent`,
    `transporter-gcs-to-gcs-agent`,
    `transporter-sftp-to-gcs-agent`
  )

/** Definitions used by both the manager and agents. */
lazy val `transporter-common` = project
  .in(file("./common"))
  .enablePlugins(BasePlugin)
  .settings(publish / skip := true)
  .settings(
    libraryDependencies ++= Seq(
      "com.beachape" %% "enumeratum" % enumeratumVersion,
      "com.beachape" %% "enumeratum-circe" % enumeratumCirceVersion,
      "com.github.pureconfig" %% "pureconfig" % pureConfigVersion,
      "com.github.pureconfig" %% "pureconfig-cats" % pureConfigVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-derivation" % circeDerivationVersion,
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
  .enablePlugins(TransporterDeployPlugin)
  .dependsOn(`transporter-common`, `transporter-manager-migrations` % Test)
  .settings(
    // Needed to resolve JSON schema lib.
    resolvers += "Jitpack" at "https://jitpack.io",
    publish := {
      publish.value
      (`transporter-manager-migrations` / publish).value
    },
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.github.everit-org.json-schema" % "org.everit.json.schema" % everitJsonSchemaVersion,
      "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion,
      "com.ovoenergy" %% "fs2-kafka" % fs2KafkaVersion,
      "com.softwaremill.tapir" %% "tapir-core" % tapirVersion,
      "com.softwaremill.tapir" %% "tapir-json-circe" % tapirVersion,
      "com.softwaremill.tapir" %% "tapir-http4s-server" % tapirVersion,
      "com.softwaremill.tapir" %% "tapir-openapi-docs" % tapirVersion,
      "com.softwaremill.tapir" %% "tapir-openapi-circe-yaml" % tapirVersion,
      "io.chrisdavenport" %% "log4cats-slf4j" % log4catsVersion,
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
    buildInfoKeys ++= Seq(
      "swaggerVersion" -> swaggerUiVersion,
      "swaggerLibrary" -> swaggerUiModule
    )
  )

/** Common framework for transfer-executing agents. */
lazy val `transporter-agent-template` = project
  .in(file("./agents/template"))
  .enablePlugins(BasePlugin)
  .dependsOn(`transporter-common`)
  .settings(publish / skip := true)
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion,
      "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
      "org.http4s" %% "http4s-blaze-server" % http4sVersion,
      "org.http4s" %% "http4s-circe" % http4sVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.log4s" %% "log4s" % log4sVersion
    ),
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-literal" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.scalamock" %% "scalamock" % scalaMockVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion,
      // NOTE: jakarta.ws.rs-api is the same project as javax.ws.rs-api, they just changed their
      // organization ID. Something in how the change happend runs across a bug in sbt / coursier,
      // so we have to manually exclude the old name and pull in the new name.
      // See: https://github.com/sbt/sbt/issues/3618
      "io.github.embeddedkafka" %% "embedded-kafka-streams" % kafkaVersion exclude ("javax.ws.rs", "javax.ws.rs-api"),
      "jakarta.ws.rs" % "jakarta.ws.rs-api" % "2.1.6"
    ).map(_ % Test)
  )

/** Agent which can transfer files from AWS to GCP. */
lazy val `transporter-aws-to-gcp-agent` = project
  .in(file("./agents/aws-to-gcp"))
  .enablePlugins(TransporterDeployPlugin)
  .dependsOn(`transporter-agent-template`)
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
  .settings(
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-blaze-client" % http4sVersion,
      "org.broadinstitute.monster" %% "gcs-lib" % storageLibsVersion
    ),
    libraryDependencies ++= Seq(
      "org.scalamock" %% "scalamock" % scalaMockVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % Test)
  )

lazy val `transporter-sftp-to-gcs-agent` = project
  .in(file("./agents/sftp-to-gcs"))
  .enablePlugins(TransporterDeployPlugin)
  .dependsOn(`transporter-agent-template`)
  .settings(
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-blaze-client" % http4sVersion,
      "org.broadinstitute.monster" %% "gcs-lib" % storageLibsVersion,
      "org.broadinstitute.monster" %% "sftp-lib" % storageLibsVersion
    ),
    libraryDependencies ++= Seq(
      "org.scalamock" %% "scalamock" % scalaMockVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % Test)
  )
