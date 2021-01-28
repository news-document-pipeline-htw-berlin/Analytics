name := "Analytics"

version := "0.1"

scalaVersion := "2.11.12"

mainClass in(Compile, run) := Some("App")
mainClass in(Compile, packageBin) := Some("App")

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.7",
  "org.apache.spark" %% "spark-sql" % "2.4.7" ,
  "org.apache.spark" %% "spark-mllib" % "2.4.7",
  "junit" % "junit" % "4.12",
  "org.scalactic" %% "scalactic" % "3.0.8" ,
  "org.scalatest" %% "scalatest" % "3.0.8" )
libraryDependencies ++= Seq(
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.7.1")
libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.2")
libraryDependencies ++= Seq(
  "org.tensorflow" %% "spark-tensorflow-connector" % "1.15.0" )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

