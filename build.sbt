name := "Analytics"

version := "0.1"

scalaVersion := "2.11.12"

mainClass in (Compile, run) := Some("App")
mainClass in (Compile, packageBin) := Some("App")

libraryDependencies ++=Seq("org.apache.spark" %% "spark-core" % "2.4.7" %"provided" ,
  "org.apache.spark" %% "spark-sql" % "2.4.7" %"provided" ,
  "org.apache.spark" %% "spark-mllib" % "2.4.7" %"provided" ,
  "junit" % "junit" % "4.12" %"provided" ,
  "org.scalactic" %% "scalactic" % "3.0.8" %"provided" ,
  "org.scalatest" %% "scalatest" % "3.0.8" % "test")
libraryDependencies ++=Seq(
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.7.1" %"provided" )
libraryDependencies ++=Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.2" %"provided" )
libraryDependencies ++= Seq("org.tensorflow" %% "spark-tensorflow-connector" % "1.15.0" %"provided" )



