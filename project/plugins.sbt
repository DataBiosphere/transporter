// Inject build variables into app code.
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
// App packaging.
addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.19")
// Code formatting.
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.0.2")
// Enable git access in the build.
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
// Run from sbt as a background process.
addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")
