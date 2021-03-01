import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object TextSumFromFullArticle {

  /**
   * Get the Data from Sparksession and apply the TextSum algorithm
   *
   * @param df
   * @param spark
   * @return
   */
  def getData(df: DataFrame, spark: SparkSession): DataFrame = {
    applyTextSum(df, spark)
  }

  /**
   * This methods splits text into sentences by using regex.
   *
   * @param input
   * @return
   */
  //TODO: Check if Date formats are identified correctly. e.g. the dot in "17. Januar" should not result into new Sentence. 17. Januar
  def splitSentences(input: String) = {
    //regex splits sentences
    input
      .split("(?<!\\w\\.\\w.)(?<![A-Z][a-z]\\.)(?<=\\.|\\?)\\s")
      .toList
      .zipWithIndex
      .map(x => x.swap)
  }

  /**
   * This method tokenizes sentences into tokens.
   * @param input
   * @return
   */
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

  /**
   * This method identifies the three most important sentences in an article. The identification happens through
   * keywords. Sentences that contain the most keywords are best equipped to represent the content of a text. This makes
   * this algorithm the easiest way to summarize a text.
   * @param article
   * @param keys
   * @param id
   * @return
   */
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

  /**
   * Apply Method for TextSum
   * @param data
   * @param spark
   * @return
   */
  def applyTextSum(data: DataFrame, spark: SparkSession): DataFrame = {

    val textSum_rdd = data.select("long_url", "text", "keywords_extracted.result").distinct.rdd.map(x => textSum(x.getAs[String](1), x.getAs[mutable.WrappedArray[String]](2).toList, x.getAs[String](0)))
    val textSum_df =spark.createDataFrame(textSum_rdd).toDF("long_url", "textSum")

    textSum_df.join(data, Seq("long_url"), joinType = "outer")

  }

}
