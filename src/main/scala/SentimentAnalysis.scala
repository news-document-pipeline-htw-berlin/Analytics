import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class SentimentAnalysis(spark: SparkSession) {
 import spark.implicits._


 val sentiments: Map[String, Double] = fileLoader("SentimentMerged.txt")

 def fileLoader(path: String): Map[String, Double] ={
  val url = getClass.getResource("/" + path).getPath
  val src = scala.io.Source.fromFile(url)
  val iter = src.getLines().toList
  val result = sentimentListToMap(iter)
  src.close()
  result
 }

 def sentimentListToMap(input: List[String]): Map[String,Double] ={
  input
     .map{x => x.replaceAll("\\|[A-Z]*\\s"," ")} //remove word class code ("|NN","|ADJX")
     .map{x => x.replaceAll(","," ")} //replace "," with space
     .map{x => x.split("\\s")} //split by space -> Array[String]
     .map{x => (x(0),x(1),x.tail.tail)} //(Basic Form:String, SentValue:String, Inflections:List[String])
     .map{x => (x._2,x._3++List(x._1))} //Tuple: Value, List of Inflections+Basicform
     .map(x => (x._2.map(y => (x._1,y)))) // Tuple (Key, Value)
     .flatten
     .map(x => x.swap)
     .map(x => (x._1,x._2.toDouble))
     .toMap
 }


 def analyseSentens(data: DataFrame):Unit ={
 val sentiments_ = sentiments

 val sum =data.select("lemmatizer.result").map(x => x.getAs[mutable.WrappedArray[String]](0).map(y => sentiments_.getOrElse(y, 0.0)).sum)
  //sum.foreach(x => println(x))
 }
}
