import java.nio.file.Files

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class SentimentAnalysis(spark: SparkSession) {

  import spark.implicits._

  val sentiments: Map[String, Double] = fileLoader("SentimentMerged.txt")

  def fileLoader(path: String): Map[String, Double] = {
    val src = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/SentimentMerged.txt"))
    val iter = src.getLines().toList
    val result = sentimentListToMap(iter)
    src.close()
    result
  }

  def sentimentListToMap(input: List[String]): Map[String, Double] = {
    input
      .map { x => x.replaceAll("\\|[A-Z]*\\s", " ") }   //remove word class code ("|NN","|ADJX")
      .map { x => x.replaceAll(",", " ") }              //replaces "," with space
      .map { x => x.split("\\s") }                                  //split by space -> Array[String]
      .map { x => (x(0), x(1), x.tail.tail) }                             //(Basic Form:String, SentValue:String, Inflections:List[String])
      .map { x => (x._2, x._3 ++ List(x._1)) }                            //Tuple: Value, List of Inflections+Basicform
      .map(x => (x._2.map(y => (x._1, y))))                               // Tuple (Key, Value)
      .flatten
      .map(x => x.swap)
      .map(x => (x._1, x._2.toDouble))
      .toMap
  }

  /**
   * this method will perform the sentiment analysis on each sentence, summing the results in the very end.
   *
   * the calculated result will be passed to a sigmoid function,
   * that will limit the result to an interval of (0.0; 5.0).
   * A value lower than 2.5 represents a text with negative connotation, while
   * a value greater than 2.5 is assigned to texts with an overall positive choice of words.
   */
  def analyseSentence(data: DataFrame): DataFrame = {

    val sentiments_ = sentiments

    val sum = data
        .select("lemmatizer.result", "long_url")
        .map(x => (x.getAs[String](1), x.getAs[mutable.WrappedArray[String]](0)
        .map(y => sentiments_.getOrElse(y, 0.0)).sum))
        .map(z => (z._1, ( java.lang.Math.exp(z._2)/ (java.lang.Math.exp(z._2) + 1.0)) * 5.0)) //modified sigmoid function, scaled by a factor of 5 to spread results for better classification
      .toDF("long_url", "sentimens")

    val data_sentiments = data.join(sum, Seq("long_url"), joinType = "outer")

    val read_time = data_sentiments
      .select("token.result", "long_url")
      .map(x => (x.getAs[String](1), x.getAs[mutable.WrappedArray[String]](0).size * 0.5))
      .toDF("long_url", "read_time")

    data_sentiments.join(read_time, Seq("long_url"), joinType = "outer")
  }
}
