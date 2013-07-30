
name := "My Project"
  
version := "0.1"
  
scalaVersion := "2.10.2"

scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "play" %% "play-iteratees" % "2.1.0"

libraryDependencies += "com.typesafe" % "config" % "1.0.2"
