import sbt._
import sbt.Keys._

import scala.util.Properties
import SonatypeSupport._

object EventStoreBuild extends Build {
  Properties.setProp("logback.configurationFile", "config/logback-unit-tests.xml")
  val buildOrganisation = "org.freetrm"
  val project = "eventstore"
  val buildVersion = "0.1-SNAPSHOT"
  val buildScalaVersion = "2.11.8"
  val license = Apache2

  val akkaV = "2.4.4"
  val sprayV = "1.3.3"

  lazy val buildSettings = Defaults.coreDefaultSettings ++
    sonatype(buildOrganisation, project, license) ++
    Seq(
      updateOptions := updateOptions.value.withCachedResolution(cachedResoluton = true),
      organization := buildOrganisation,
      version := buildVersion,
      name := project,
      scalaVersion := buildScalaVersion,
      scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-Xfatal-warnings")
    )

  lazy val base = module("base", withResources = true).settings(
    libraryDependencies ++=
      "ch.qos.logback" % "logback-classic" % "1.1.7" +: 
      "log4j" % "log4j" % "1.2.17" +: 
      DepSeq(
        "com.typesafe" % "config" % "1.3.0",
        "org.scaldi" %% "scaldi" % "0.5.7",
        "org.scaldi" %% "scaldi-akka" % "0.5.7",
      
        "com.github.stacycurl" %% "pimpathon-core" % "1.5.0",
        "org.scalaz" %% "scalaz-core" % "7.2.2",
        "com.chuusai" %% "shapeless" % "2.3.0",

        "com.typesafe.akka" %% "akka-actor" % akkaV,
        "com.typesafe.akka" %% "akka-stream" % akkaV,
        "com.typesafe.akka" %% "akka-http-core" % akkaV,
        "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaV,
        "de.heikoseeberger" %% "akka-sse" % "1.7.4",

        "io.spray" %% "spray-json" % "1.3.2",
        "com.github.fommil" %% "spray-json-shapeless" % "1.2.0",
    
        "io.netty" % "netty" % "3.10.5.Final",

        "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test",
        "org.scalatest" %% "scalatest" % "2.2.6" % "test",

        "com.typesafe.akka" %% "akka-http-testkit" % akkaV % "test",
        "com.typesafe.akka" %% "akka-stream-testkit" % akkaV % "test"
      ),
      resolvers += "softprops-maven" at "http://dl.bintray.com/content/softprops/maven"
  )


  lazy val db = module("db", withResources = true).settings(
    libraryDependencies ++=
      "commons-logging" % "commons-logging" % "1.2" +:
        DepSeq(
          "org.suecarter" %% "freeslick" % "3.1.1.1",
          "com.typesafe.slick" %% "slick-hikaricp" % "3.1.1",
          "com.zaxxer" % "HikariCP" % "2.4.6",
          "com.h2database" % "h2" % "1.4.191" % "optional",
          "net.sourceforge.jtds" % "jtds" % "1.2.8" % "optional"
        )).dependsOn(
    base % "compile->compile;test->test"
  )

  def module(name: String, withResources: Boolean = false): Project = {
    Project(
      id = name,
      base = file(name),
      settings = buildSettings
    )
      .settings(
        scalaSource in Compile := baseDirectory.value / "src",
        scalaSource in Test := baseDirectory.value / "tests",
        resourceDirectory in Test := baseDirectory.value / "test-resources"
      )
      .settings(Seq(
      withResources.option(resourceDirectory in Compile := baseDirectory.value / "resources")
    ).flatten: _*
      )
  }

  def DepSeq(modules: ModuleID*): Seq[ModuleID] = {
    modules.map {
      _.exclude("org.slf4j", "slf4j-log4j12")
        .exclude("com.sun.jdmk", "jmxtools")
        .exclude("log4j", "log4j") // from apache curator and 101tec zkclient
        .exclude("javax.jms", "jms")
        .exclude("com.sun.jmx", "jmxri")
        .exclude("commons-logging", "commons-logging")
    }
  }

  implicit class RichBoolean(val b: Boolean) extends AnyVal {
    final def option[A](a: => A): Option[A] = if (b) Some(a) else None
  }
}
