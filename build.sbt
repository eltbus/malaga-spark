ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.10"

Test / fork := true

lazy val root = (project in file("."))
  .settings(
    name := "dummy-spark",
  )

val sparkVersion = "2.4.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
