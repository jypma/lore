import ScalaxbKeys._

name := "lore"

version := "1.0"

scalaVersion := "2.10.0-M6"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.1-M1"

libraryDependencies += "com.typesafe.akka" % "akka-camel" % "2.1-M1"

EclipseKeys.withSource := true

EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.Resource

seq(scalaxbSettings: _*)

sourceGenerators in Compile <+= scalaxb in Compile

packageNames in scalaxb in Compile := Map(
  uri("urn:un:unece:uncefact:data:specification:CoreComponentTypeSchemaModule:2") -> "un.ccts",
  uri("urn:oasis:names:specification:ubl:schema:xsd:ApplicationResponse-2") -> "ubl.ar",
  uri("urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2") -> "ubl.cac",
  uri("urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2") -> "ubl.cbc",
  uri("urn:oasis:names:specification:ubl:schema:xsd:CommonExtensionComponents-2") -> "ubl.cec",
  uri("urn:oasis:names:specification:ubl:schema:xsd:CommonSignatureComponents-2") -> "ubl.ar",
  uri("urn:un:unece:uncefact:documentation:2") -> "ubl.ccp",
  uri("urn:oasis:names:specification:ubl:schema:xsd:ExtensionContentDataType-2") -> "ubl.ecd",
  uri("urn:oasis:names:specification:ubl:schema:xsd:QualifiedDataTypes-2") -> "ubl.qdt",
  uri("urn:oasis:names:specification:ubl:schema:xsd:SignatureAggregateComponents-2") -> "ubl.sac",
  uri("urn:oasis:names:specification:ubl:schema:xsd:SignatureBasicComponents-2") -> "ubl.sbc",
  uri("urn:oasis:names:specification:ubl:schema:xsd:UnqualifiedDataTypes-2") -> "ubl.udt",
  uri("http://www.w3.org/2000/09/xmldsig#") -> "org.w3c.xmldsig"
)
