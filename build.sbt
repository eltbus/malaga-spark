ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.18"


lazy val root = (project in file("."))
  .settings(
    name := "dummy-spark",
  )

val sparkVersion = "3.5.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

// Test
Test / fork := true
Test / envVars := Map("JAVA_OPTS" -> "--add-exports java.base/sun.nio.ch=ALL-UNNAMED")
Test / javaOptions ++= Seq(
  "-Dlog4j.configuration=file:src/test/resources/log4j.properties"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % Test