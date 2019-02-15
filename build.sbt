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

// Typelevel stack.
val catsVersion = "1.6.0"
val catsEffectVersion = "1.2.0"

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
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % catsEffectVersion
    ),
    libraryDependencies ++= Seq(
      "com.dimafeng" %% "testcontainers-scala" % testcontainersScalaVersion,
      "org.liquibase" % "liquibase-core" % liquibaseVersion,
      "org.postgresql" % "postgresql" % postgresqlDriverVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion,
      "org.testcontainers" % "postgresql" % testcontainersVersion
    ).map(_ % Test),
    dependencyOverrides := Seq(
      "org.testcontainers" % "testcontainers" % testcontainersVersion,
      "org.typelevel" %% "cats-core" % catsVersion
    )
  )

lazy val `transporter-agent-template` = project
  .in(file("./agents/template"))
  .settings(commonSettings)
