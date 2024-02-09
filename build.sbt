ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.13"

Test / fork := true

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(
    name := "dummy-spark",
  )

val sparkVersion = "3.5.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"