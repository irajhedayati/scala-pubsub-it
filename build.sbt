ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "scala-pubsub-it",
    libraryDependencies += "com.google.cloud" % "google-cloud-pubsub" % "1.119.1"
  )
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest"                   % "3.2.12" % "it",
      "com.dimafeng"  %% "testcontainers-scala"        % "0.40.9" % "it",
      "com.dimafeng"  %% "testcontainers-scala-gcloud" % "0.40.9" % "it"
    ),
    IntegrationTest / fork := true
  )
