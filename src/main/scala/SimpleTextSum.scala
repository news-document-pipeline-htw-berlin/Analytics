import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable

class SimpleTextSum(spark: SparkSession) {

  import spark.implicits._


  /**
   * Takes the article split by sentence Gives you the three sentences that contain the most keywords. The idea behind this very simple TextSum algorithm is
   * that sentences that contain many keywords describe the content of an article very well. The result is often not
   * pleasant to read, but the relation between computational effort and accuracy is very good.
   * @param keywords
   * @param articleAsTokenSentenceIdTuples
   * @param articleSplitBySentence
   * @return
   */
  def getTopSentences(id: String, keywords:List[String], articleAsTokenSentenceIdTuples :List[(Int,String)], articleSplitBySentence:Map[Int,String]):(String,List[String]) = {
    val topSentences =
    articleAsTokenSentenceIdTuples
      .map{case (sentenceID,token) => (sentenceID, keywords.contains(token))}
      .filter{case (sentenceID,isKeyword) => isKeyword} //remove false
      .groupBy{case (sentenceID,containsKeywordBoolean) => sentenceID} //GroupBy SentenceID
      .map{case (sentenceId,listOfContainsKeywordBoolean) => (sentenceId,listOfContainsKeywordBoolean.size) }
      .toSeq //From Map to Sequence
      .sortBy{case (sentenceId,amountOfKeywordsContained) => -amountOfKeywordsContained} //Sort by Size Descending
      .take(3) //Take Top3 SentenceID
      .map{case (sentenceId,amountOfKeywordsContained) => articleSplitBySentence.getOrElse(sentenceId,"")} //Take Top Three Sentences from "full Map"
      .toList

    (id,topSentences)

  }

  /**
   * applies the algorithm to given DataFrame and returns Dataset with id and TopSentences
   * @param df
   * @return
   */
  //ToDo: getAs must be corrected so the right part of the dataFrame is taken. Maybe Datatypes need to be adjusted as well.
  def applyGetTopSentences(df:DataFrame):Dataset[(String,List[String])]={
    df.select("tokenizer.result","_id")
      .map(x => ( x.getAs[String](1),x.getAs[List[String]](2),x.getAs[List[(Int,String)]](3),x.getAs[Map[Int,String]](4)))
      .map{case (id, keywords,articleAsTokenSentenceIdTuples,articleSplitBySentence) => getTopSentences(id,keywords,articleAsTokenSentenceIdTuples,articleSplitBySentence)}
  }

}
