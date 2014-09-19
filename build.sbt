name := "lore"

version := "1.0"

scalaVersion := "2.11.2"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.2" 

libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % "2.3.2" 

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.3.2" % "test"
            
libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.3" % "test"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.0.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.0.2",
  "org.apache.logging.log4j" % "log4j-api" % "2.0.2"
)
 
EclipseKeys.withSource := true

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource
