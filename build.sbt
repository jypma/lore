name := "lore"

version := "1.0"

scalaVersion := "2.10.0"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.10" % "2.1.0" 

libraryDependencies += "com.typesafe.akka" % "akka-testkit_2.10" % "2.1.0" % "test"
            
libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0.M5b" % "test"

libraryDependencies += "com.typesafe" % "scalalogging-slf4j_2.10" % "1.0.1"
            

EclipseKeys.withSource := true

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource
