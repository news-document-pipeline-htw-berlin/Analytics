import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class TextSumFromFullArticle(spark: SparkSession) {

  import spark.implicits._


  def getData(df: DataFrame): DataFrame = {
    TextSumFromFullArticle.applyTextSum(df.select("_id", "keywords_extracted.result", "text"))

  }

  object TextSumFromFullArticle{
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
      //println(articleSplitBySentence)
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
          .mkString("")

      (id, output)
    }

    def applyTextSum(data: DataFrame): DataFrame = {

      val kwaTriple = data.map(x => (x.getString(0), x.getAs[mutable.WrappedArray[String]](2), x.getString(1)))

      kwaTriple.map { case (id, keys, article) => textSum(id, keys.toList, article) }.toDF("_id", "textsum")

    }
  }
}
