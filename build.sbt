
name := "art"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "io.argonaut" %% "argonaut" % "6.0.4",
  "org.apache.spark" %% "spark-streaming" % "1.3.1",
  "oncue.svc.remotely" %% "core" % "1.3.0-41"
)