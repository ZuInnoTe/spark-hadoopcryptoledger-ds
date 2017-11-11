

import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
       organization := "com.github.zuinnote",
    name := "spark-hadoopcryptoledger-ds",
    version := "1.1.0"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)


publishTo := Some(Resolver.file("file",  new File(Path.userHome.absolutePath+"/.m2/repository")))

crossScalaVersions := Seq("2.10.5", "2.11.7")

scalacOptions += "-target:jvm-1.7"

javaOptions += "-Xmx3G"

fork := true

resolvers += Resolver.mavenLocal


jacoco.settings

itJacoco.settings



libraryDependencies += "com.github.zuinnote" % "hadoopcryptoledger-fileformat" % "1.1.0" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0" % "provided"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.4.1" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"


libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-minicluster" % "2.7.0" % "it"
