ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.8.2"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-aggregator",
    version := "0.1.0",
    Compile / mainClass := Some("kafka.telem.agg.Main"),
    libraryDependencies ++= Seq(
      "com.typesafe.play" %% "play-json" % "2.10.4",
      "org.apache.kafka" % "kafka-clients" % "3.7.0",
      "com.typesafe" % "config" % "1.4.3",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    Test / parallelExecution := false
  )