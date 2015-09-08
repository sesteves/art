
name := "art"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "io.argonaut" %% "argonaut" % "6.0.4",
  "org.apache.spark" %% "spark-streaming" % "1.3.1",
  "org.apache.spark" % "spark-mllib_2.10" % "1.3.1",
  "nz.ac.waikato.cms.weka" % "weka-dev" % "3.7.12"
)