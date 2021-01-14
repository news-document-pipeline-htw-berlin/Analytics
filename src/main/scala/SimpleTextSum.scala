import org.apache.spark.sql.{DataFrame, SparkSession}

class SimpleTextSum(spark: SparkSession) {

  import spark.implicits._

  /**
   * applies the algorithm to given DataFrame and returns Dataset with id and TopSentences
   *
   * @param df
   * @return
   */
  //ToDo: getAs must be corrected so the right part of the dataFrame is taken. Maybe Datatypes need to be adjusted as well.
  def applyGetTopSentences(df: DataFrame): Unit = {
    df.select("document.result").limit(1).show(false)
    val sum = df.select("token", "_id")
      .map(x => (x.getAs[String](1), x.getAs[String](0), x.getAs[Map[String, Int]](0)("sentence")))

    //df.join(sum, Seq("_id"), joinType = "outer")
  }

  object TopSentences{
    /**
     * Takes the article split by sentence Gives you the three sentences that contain the most keywords. The idea behind this very simple TextSum algorithm is
     * that sentences that contain many keywords describe the content of an article very well. The result is often not
     * pleasant to read, but the relation between computational effort and accuracy is very good.
     *
     * @param keywords
     * @param articleAsTokenSentenceIdTuples
     * @param articleSplitBySentence
     * @return
     */
    def getTopSentences(id: String, keywords: List[String], articleAsTokenSentenceIdTuples: List[(Int, String)], articleSplitBySentence: Map[Int, String]): (String, String) = {
      val topSentences =
        articleAsTokenSentenceIdTuples
          .map { case (sentenceID, token) => (sentenceID, keywords.contains(token)) }
          .filter { case (sentenceID, isKeyword) => isKeyword } //remove false
          .groupBy { case (sentenceID, containsKeywordBoolean) => sentenceID } //GroupBy SentenceID
          .map { case (sentenceId, listOfContainsKeywordBoolean) => (sentenceId, listOfContainsKeywordBoolean.size) }
          .toSeq //From Map to Sequence
          .sortBy { case (sentenceId, amountOfKeywordsContained) => -amountOfKeywordsContained } //Sort by Size Descending
          .take(3) //Take Top3 SentenceID
          .map { case (sentenceId, amountOfKeywordsContained) => articleSplitBySentence.getOrElse(sentenceId, "") } //Take Top Three Sentences from "full Map"
          .toList

      (id, topSentences.mkString(""))

    }
  }
}
