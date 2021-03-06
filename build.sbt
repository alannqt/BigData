name := "bigdata"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.5"

resolvers ++= Seq(   "apache-snapshots" at "http://repository.apache.org/snapshots/" )

libraryDependencies ++= Seq(   "mysql" % "mysql-connector-java" % "5.1.+",   "org.apache.spark" %% "spark-core" % sparkVersion,   "org.apache.spark" %% "spark-sql" % sparkVersion,   "org.apache.spark" %% "spark-mllib" % sparkVersion,   "org.apache.spark" %% "spark-streaming" % sparkVersion,   "org.apache.spark" %% "spark-hive" % sparkVersion )
