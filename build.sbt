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
val betterMonadicForVersion = "0.2.4"

// Configuration.
val pureConfigVersion = "0.10.2"

// Data types & control flow.
val catsVersion = "1.6.0"
val catsEffectVersion = "1.2.0"
val fs2Version = "1.0.4"

// DB.
val doobieVersion = "0.7.0-M3"
val postgresqlDriverVersion = "42.2.5"

// JSON.
val circeVersion = "0.11.1"
val circeDerivationVersion = "0.11.0-M1"
val enumeratumCirceVersion = "1.5.21"
val everitJsonSchemaVersion = "1.11.1"

// Kafka.
val fs2KafkaVersion = "0.19.4"
val kafkaVersion = "2.1.1"

// Logging.
val logbackVersion = "1.2.3"
val log4catsVersion = "0.3.0"

// Utils.
val enumeratumVersion = "1.5.13"
val fuuidVersion = "0.2.0-M7"

// Web.
val http4sVersion = "0.20.0-M7"
val rhoVersion = "0.19.0-M6"
val swaggerUiModule = "swagger-ui"
val swaggerUiVersion = "3.20.9"

// Testing.
val liquibaseVersion = "3.6.3"
val scalaMockVersion = "4.1.0"
val scalaTestVersion = "3.0.7"
val testcontainersVersion = "1.10.7"
val testcontainersScalaVersion = "0.23.0"

// Settings to apply to all sub-projects.
// Can't be applied at the build level because of scoping rules.
val commonSettings = Seq(
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % betterMonadicForVersion),
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

lazy val transporter = project
  .in(file("."))
  .aggregate(
    `transporter-common`,
    `transporter-manager`,
    `transporter-agent-template`,
    `transporter-echo-agent`
  )

/** Definitions used by both the manager and agents. */
lazy val `transporter-common` = project
  .in(file("./common"))
  .settings(commonSettings)
  .settings(
    // Needed to resolve JSON schema lib.
    resolvers += "Jitpack" at "https://jitpack.io",

    libraryDependencies ++= Seq(
      "com.beachape" %% "enumeratum" % enumeratumVersion,
      "com.beachape" %% "enumeratum-circe" % enumeratumCirceVersion,
      "com.github.everit-org.json-schema" % "org.everit.json.schema" % everitJsonSchemaVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-derivation" % circeDerivationVersion,
      "io.circe" %% "circe-parser" % circeVersion
    ),

    libraryDependencies ++= Seq(
      "io.circe" %% "circe-literal" % circeVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % Test)
  )

/** Web service which receives, distributes, and tracks transfer requests. */
lazy val `transporter-manager` = project
  .in(file("./manager"))
  .enablePlugins(BuildInfoPlugin)
  .dependsOn(`transporter-common`)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.github.pureconfig" %% "pureconfig" % pureConfigVersion,
      "com.github.pureconfig" %% "pureconfig-cats" % pureConfigVersion,
      "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion,
      "com.ovoenergy" %% "fs2-kafka" % fs2KafkaVersion,
      "io.chrisdavenport" %% "fuuid" % fuuidVersion,
      "io.chrisdavenport" %% "log4cats-slf4j" % log4catsVersion,
      "org.http4s" %% "http4s-blaze-server" % http4sVersion,
      "org.http4s" %% "http4s-circe" % http4sVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.http4s" %% "rho-swagger" % rhoVersion,
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
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io" % fs2Version,
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      "org.postgresql" % "postgresql" % postgresqlDriverVersion,
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion,

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
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.github.pureconfig" %% "pureconfig" % pureConfigVersion,
      "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion,
      "com.github.pureconfig" %% "pureconfig-http4s" % pureConfigVersion,
      "io.chrisdavenport" %% "log4cats-slf4j" % log4catsVersion,
      "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
      "org.http4s" %% "http4s-blaze-client" % http4sVersion,
      "org.http4s" %% "http4s-circe" % http4sVersion
    ),

    libraryDependencies ++= Seq(
      "io.circe" %% "circe-literal" % circeVersion,
      "io.github.embeddedkafka" %% "embedded-kafka-streams" % kafkaVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion
    ).map(_ % Test),

    dependencyOverrides := Seq(
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io" % fs2Version,
      "org.apache.kafka" % "kafka-clients" % kafkaVersion,
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
    )
  )

lazy val `transporter-echo-agent` = project
  .in(file("./agents/echo"))
  .dependsOn(`transporter-agent-template`)
  .settings(commonSettings)
