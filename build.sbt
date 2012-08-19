name := "lore"

version := "1.0"

scalaVersion := "2.10.0-M6"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.1-M1"

libraryDependencies += "com.typesafe.akka" % "akka-camel" % "2.1-M1"

//libraryDependencies += "com.typesafe.akka" % "akka-transactor" % "2.0.2"

//libraryDependencies += "com.typesafe.akka" % "akka-agent" % "2.0.2"

//libraryDependencies += "com.typesafe.akka" % "akka-file-mailbox" % "2.0.2"

//libraryDependencies += "com.typesafe.akka" % "akka-remote" % "2.0.2"

EclipseKeys.withSource := true

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource
