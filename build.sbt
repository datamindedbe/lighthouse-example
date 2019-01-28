name := "lighthouse-example"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion     = "2.3.1"

val sparkCore = "org.apache.spark" %% "spark-core"  % sparkVersion % Provided
val sparkSql  = "org.apache.spark" %% "spark-sql"   % sparkVersion % Provided
val sparkHive = "org.apache.spark" %% "spark-hive"  % sparkVersion % Provided

libraryDependencies ++= Seq(sparkCore, sparkSql, sparkHive)

libraryDependencies += "be.dataminded" %% "lighthouse" % "0.2.3"
libraryDependencies += "be.dataminded" %% "lighthouse-testing" % "0.2.3" % Test

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % Test



