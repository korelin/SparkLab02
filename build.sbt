name := "lab02"
version := "0.1"
//scalaVersion := "2.12.7"
scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.apache.spark" %% "spark-mllib" % "2.4.5",
  "io.circe" %% "circe-core" % "0.12.0-M3",
  "io.circe" %% "circe-generic" % "0.12.0-M3",
  "io.circe" %% "circe-parser" % "0.12.0-M3"
)

//addCompilerPlugin(
//  "org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full
//)