name := "billing-initiator"

version := "0.1"

scalaVersion := "2.13.3"

// JFrog Config
credentials += Credentials(new File("./local/credentials.properties"))
publishTo := Some("Artifactory Realm" at "https://farther.jfrog.io/artifactory/sbt-dev;build.timestamp=" + new java.util.Date().getTime)
resolvers += "Artifactory" at "https://farther.jfrog.io/artifactory/sbt-dev/"

libraryDependencies += "com.typesafe" % "config" % "1.4.0"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % "test"

libraryDependencies += "com.softwaremill.sttp.client" %% "core" % "2.1.0-RC1"

libraryDependencies += "com.typesafe.slick" %% "slick" % "3.3.2"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.16"
libraryDependencies += "com.typesafe.slick" %% "slick-hikaricp" % "3.3.2"
libraryDependencies += "ch.qos.logback"      % "logback-classic" % "1.2.3"

libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.6"

libraryDependencies += "com.farther" %% "northstardb" % "0.2.1-SNAPSHOT"
libraryDependencies += "com.farther" %% "grecoevents" % "0.9.13-SNAPSHOT"
libraryDependencies += "com.farther" %% "pubsub" % "0.2-SNAPSHOT"