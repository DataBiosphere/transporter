// Inject build variables into app code.
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
// Code formatting.
addSbtPlugin("com.geirsson" % "sbt-scalafmt" % "1.6.0-RC4")
// Enable git access in the build.
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
// Parallelize dependnecy resolution / download.
addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.1.0-M9")

