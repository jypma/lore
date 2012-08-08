name := "lore"

version := "1.0"

scalaVersion := "2.9.1"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.2"

//libraryDependencies += "com.typesafe.akka" % "akka-transactor" % "2.0.2"

//libraryDependencies += "com.typesafe.akka" % "akka-agent" % "2.0.2"

//libraryDependencies += "com.typesafe.akka" % "akka-file-mailbox" % "2.0.2"

//libraryDependencies += "com.typesafe.akka" % "akka-remote" % "2.0.2"

EclipseKeys.withSource := true