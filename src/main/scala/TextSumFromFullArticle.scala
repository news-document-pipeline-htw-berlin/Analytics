import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object TextSumFromFullArticle {

  def getData(df: DataFrame, spark: SparkSession): DataFrame = {

    applyTextSum(df, spark)
  }


  def splitSentences(input: String) = {
    //regex splits sentences
    input
      .split("(?<!\\w\\.\\w.)(?<![A-Z][a-z]\\.)(?<=\\.|\\?)\\s")
      .toList
      .zipWithIndex
      .map(x => x.swap)
  }

  def tokenize(input: (Int, String)): List[(Int, String)] = {

    input._2
      .replace(",", "")
      .replace(".", "")
      .replace("!", "")
      .replace("?", "")
      .replace(";", "")
      .replace("-", "")
      .split("\\s")
      .toList
      .map(x => (input._1, x))

  }

  def textSum(article: String, keys: List[String], id: String): (String, String) = {

    val lowerCaseKeys = keys.map(x => x.toLowerCase)
    val articleSplitBySentence = splitSentences(article)
    val articleSentencesAsMap = articleSplitBySentence.toMap
    val tokenizedSentences = articleSplitBySentence.flatMap(x => tokenize(x))

    val output =
      tokenizedSentences
        .map { case (sentenceID, token) => (sentenceID, lowerCaseKeys.contains(token.toLowerCase)) }
        .filter { case (sentenceID, isKeyword) => isKeyword } //remove false
        .groupBy { case (sentenceID, containsKeywordBoolean) => sentenceID } //GroupBy SentenceID
        .map { case (sentenceId, listOfContainsKeywordBoolean) => (sentenceId, listOfContainsKeywordBoolean.size) }
        .toSeq //From Map to Sequence
        .sortBy { case (sentenceId, amountOfKeywordsContained) => -amountOfKeywordsContained } //Sort by Size Descending
        .take(3) //Take Top3 SentenceID
        .map { case (sentenceId, amountOfKeywordsContained) => articleSentencesAsMap.getOrElse(sentenceId, "") } //Take Top Three Sentences from "full Map"
        .mkString(" ")

    (id, output)

  }

  def applyTextSum(data: DataFrame, spark: SparkSession): DataFrame = {

    val textSum_rdd = data.select("long_url", "text", "keywords_extracted.result").distinct.rdd.map(x => textSum(x.getAs[String](1), x.getAs[mutable.WrappedArray[String]](2).toList, x.getAs[String](0)))
    val  textSum_df =spark.createDataFrame(textSum_rdd).toDF("long_url", "textSum")

    textSum_df.join(data, Seq("long_url"), joinType = "outer")

  }

}
