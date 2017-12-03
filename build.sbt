import Keys._

lazy val Spark200 = config(s"spark200") extend IntegrationTest describedAs s"Integration tests against Spark 2.0.0"
lazy val Spark210 = config(s"spark210") extend IntegrationTest describedAs s"Integration tests against Spark 2.1.0"

lazy val root = (project in file("."))
  .settings(
    organization := "com.github.zuinnote",

    name := "spark-hadoopcryptoledger-ds",

    version := "1.1.2",

    scalaVersion := "2.11.8",

    crossScalaVersions := Seq("2.10.6", "2.11.8"),

    libraryDependencies ++= Seq(
      "com.github.zuinnote"       % "hadoopcryptoledger-fileformat"  % "1.1.2" % "compile",

      "org.bouncycastle"          % "bcprov-ext-jdk15on"             % "1.58"  % "provided",
      "org.apache.spark"         %% "spark-core"                     % "2.0.0" % "provided",
      "org.apache.spark"         %% "spark-sql"                      % "2.0.0" % "provided",
      "org.apache.hadoop"         % "hadoop-client"                  % "2.7.0" % "provided",
      "org.apache.logging.log4j"  % "log4j-api"                      % "2.4.1" % "provided",

      "org.scalatest"            %% "scalatest"                      % "3.0.1" % "test,it",

      "javax.servlet"             % "javax.servlet-api"              % "3.0.1" % "it",
      "org.apache.hadoop"         % "hadoop-common"                  % "2.7.0" % "it" classifier "" classifier "tests",
      "org.apache.hadoop"         % "hadoop-hdfs"                    % "2.7.0" % "it" classifier "" classifier "tests",
      "org.apache.hadoop"         % "hadoop-minicluster"             % "2.7.0" % "it"
    ),

    publishTo := Some(Resolver.file("file", new File(Path.userHome.absolutePath + "/.m2/repository"))),

    scalacOptions += "-target:jvm-1.7",

    javaOptions += "-Xmx3G",

    fork := true,

    resolvers += Resolver.mavenLocal
  )
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*)
  .configs(Spark200, Spark210)
  .settings(
    inConfig(Spark200)(
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "2.0.0" % "it",
        "org.apache.spark" %% "spark-sql"  % "2.0.0" % "it"
      )
    ),
    inConfig(Spark210)(
      libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % "2.1.0" % "it",
        "org.apache.spark" %% "spark-sql"  % "2.1.0" % "it"
      )
    )
  )
