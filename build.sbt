import Keys._

lazy val root = (project in file("."))
  .settings(
    organization := "com.github.zuinnote",

    name := "spark-hadoopcryptoledger-ds",

    version := "1.3.0",

    scalaVersion := "2.11.12",

    crossScalaVersions := Seq("2.11.12","2.12.10"),

    libraryDependencies ++= Seq(
      "com.github.zuinnote"       % "hadoopcryptoledger-fileformat"  % "1.3.0" % "compile",

      "org.bouncycastle"          % "bcprov-ext-jdk15on"             % "1.68"  % "compile",
      "org.apache.spark"         %% "spark-core"                     % "2.4.4" % "provided",
      "org.apache.spark"         %% "spark-sql"                      % "2.4.4" % "provided",
      "org.apache.hadoop"         % "hadoop-client"                  % "2.7.0" % "provided",
      "org.apache.logging.log4j"  % "log4j-api"                      % "2.14.0" % "provided",

      "org.scalatest"            %% "scalatest"                      % "3.1.0" % "test,it",

      "javax.servlet"             % "javax.servlet-api"              % "3.0.1" % "it",
      "org.apache.hadoop"         % "hadoop-common"                  % "2.7.0" % "it" classifier "" classifier "tests",
      "org.apache.hadoop"         % "hadoop-hdfs"                    % "2.7.0" % "it" classifier "" classifier "tests",
      "org.apache.hadoop"         % "hadoop-minicluster"             % "2.7.0" % "it"
    ),


    publishTo := Some(Resolver.file("file", new File(Path.userHome.absolutePath + "/.m2/repository"))),

    scalacOptions ++= Seq(
      "-target:jvm-1.8",
      "-feature", "-deprecation", "-unchecked", "-explaintypes",
      "-encoding", "UTF-8", // yes, this is 2 args
      "-language:reflectiveCalls", "-language:implicitConversions", "-language:postfixOps", "-language:existentials",
      "-language:higherKinds",
      "-Xcheckinit", "-Xexperimental", "-Xfatal-warnings", "-Xfuture", "-Xlint",
      "-Ywarn-dead-code", "-Ywarn-inaccessible", "-Ywarn-numeric-widen", "-Yno-adapted-args", "-Ywarn-unused-import",
      "-Ywarn-unused"
    ),

    javaOptions += "-Xmx3G",

    fork := true,

    resolvers += Resolver.mavenLocal
  )
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*)

  artifact in (Compile, assembly) := {
    val art = (artifact in (Compile, assembly)).value
    art.withClassifier(Some(""))
  }

  addArtifact(artifact in (Compile, assembly), assembly)

  assemblyShadeRules in assembly := Seq(
     ShadeRule.rename("org.bouncycastle.**" -> "hadoopcryptoledger.shade.org.bouncycastle.@1").inAll
  )
  assemblyJarName in assembly := {
       val newName = s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"
       newName
  }

  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

  assemblyMergeStrategy in assembly :=  {
      case PathList("META-INF/*.RSA", "META-INF/*.SF","META-INF/*.DSA") => MergeStrategy.discard
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
       oldStrategy(x)

  }
