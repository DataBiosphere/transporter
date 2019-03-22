// Inject build variables into app code.
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
// Code formatting.
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.0.0")
// Enable git access in the build.
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
// Parallelize dependency resolution / download.
addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.1.0-M13-2")
// Run from sbt as a background process.
addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")
