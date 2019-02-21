// Settings to apply across the entire build.
enablePlugins(GitVersioning)
inThisBuild(
  Seq(
    organization := "org.broadinstitute",
    scalaVersion := "2.12.8",

    // Auto-format
    scalafmtConfig := Some((ThisBuild / baseDirectory)(_ / ".scalafmt.conf").value),
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
val fs2Version = "1.0.3"

// DB.
val doobieVersion = "0.7.0-M3"

// JSON.
val circeVersion = "0.11.1"
val circeDerivationVersion = "0.11.0-M1"

// Kafka.
val fs2KafkaVersion = "0.19.1"
val kafkaClientsVersion = "2.1.0"

// Logging.
val logbackVersion = "1.2.3"

// Web.
val http4sVersion = "0.20.0-M5" // -M6 breaks rho for now (2/19/18).
val rhoVersion = "0.19.0-M5"
val swaggerUiVersion = "3.20.8"

// Testing.
val liquibaseVersion = "3.6.3"
val postgresqlDriverVersion = "42.2.5"
val scalaTestVersion = "3.0.5"
val testcontainersVersion = "1.10.6"
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
  Test / fork := true
)

lazy val transporter = project
  .in(file("."))
  .aggregate(
    `transporter-manager`,
    `transporter-agent-template`
  )

lazy val `transporter-manager` = project
  .in(file("./manager"))
  .enablePlugins(BuildInfoPlugin)
  .settings(commonSettings)
  .settings(
    // Main dependencies.
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % logbackVersion,
      "com.github.pureconfig" %% "pureconfig" % pureConfigVersion,
      "com.github.pureconfig" %% "pureconfig-cats-effect" % pureConfigVersion,
      "com.ovoenergy" %% "fs2-kafka" % fs2KafkaVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-derivation" % circeDerivationVersion,
      "org.http4s" %% "http4s-blaze-server" % http4sVersion,
      "org.http4s" %% "http4s-circe" % http4sVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.http4s" %% "rho-swagger" % rhoVersion,
      "org.tpolecat" %% "doobie-core" % doobieVersion,
      "org.tpolecat" %% "doobie-hikari" % doobieVersion,
      "org.tpolecat" %% "doobie-postgres" % doobieVersion,
      "org.webjars" % "swagger-ui" % swaggerUiVersion
    ),

    // Test dependencies.
    libraryDependencies ++= Seq(
      "com.dimafeng" %% "testcontainers-scala" % testcontainersScalaVersion,
      "org.liquibase" % "liquibase-core" % liquibaseVersion,
      "org.postgresql" % "postgresql" % postgresqlDriverVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion,
      "org.testcontainers" % "kafka" % testcontainersVersion,
      "org.testcontainers" % "postgresql" % testcontainersVersion
    ).map(_ % Test),

    // Pin transitive dependencies to avoid chaos.
    dependencyOverrides := Seq(
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io" % fs2Version,
      "org.apache.kafka" % "kafka-clients" % kafkaClientsVersion,
      "org.testcontainers" % "testcontainers" % testcontainersVersion,
      "org.typelevel" %% "cats-core" % catsVersion,
      "org.typelevel" %% "cats-effect" % catsEffectVersion
    ),

    // Inject version information into the app.
    buildInfoKeys := Seq(
      version,
      "swaggerVersion" -> swaggerUiVersion
    ),
    buildInfoPackage := "org.broadinstitute.transporter"
  )

lazy val `transporter-agent-template` = project
  .in(file("./agents/template"))
  .settings(commonSettings)
