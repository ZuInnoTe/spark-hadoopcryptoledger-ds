lazy val root = (project in file(".")).
  settings(
    organization := "com.github.zuinnote",
    name := "spark-hadoopcryptoledger-ds",
    version := "1.0.2",
    scalaVersion := "2.10.4"
  )

scalacOptions += "-target:jvm-1.7"

libraryDependencies += "com.github.zuinnote" % "hadoopcryptoledger-fileformat" % "1.0.2" % "compile"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.5.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0" % "provided"

libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.4.1" % "provided"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"

