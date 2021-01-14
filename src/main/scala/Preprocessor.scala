import java.sql.Timestamp

import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.SentenceDetector
import com.johnsnowlabs.nlp.annotators._
import com.johnsnowlabs.nlp.annotators.keyword.yake.YakeModel
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.util.io.ResourceHelper.spark
import org.apache.parquet.Strings
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.joda.time.DateTime

import scala.collection.mutable


class Preprocessor {

  /** this method will run the preprocessing pipeline
   *
   * @param data = crawled article
   */
  def run_pp(data: DataFrame):DataFrame = {
    // retrieves input data from row of Dataframe
    preprocessArticles(data)
  }

  /** helper method, extracting text body and associated ID
   *
   * @param data = crawled article(s)
   * @return key-value pairs ID -> text-body
   */
  def getRawDataFrame(data: RDD[Row]): DataFrame = {
    val data_raw=data.map(x => (
      x.get(0).toString,
      x.getAs[Array[String]](1),
      x.get(2).asInstanceOf[Timestamp],
      x.get(8).toString,
      x.get(11).toString,
      x.get(9).toString,
      x.get(13).toString,
      x.get(3).asInstanceOf[String],
      x.get(5).asInstanceOf[String],
      x.get(12).toString,
      x.getAs[Array[String]](6),
      x.get(10).asInstanceOf[Timestamp],
      x.getAs[Array[String]](4),
      x.getAs[Array[String]](7)))
    spark.createDataFrame(data_raw.collect()).toDF(
      "_id",
                "authors",
                "crawl_time",
                "longUrl",
                "short_url",
                "news_site",
                "title",
                "description",
                "intro",
                "text",
                "keywords_given",
                "published_time",
                "image_links",
                "links"
    ).limit(10)
  }

  /** this method will perform several operations of preprocessing on the incoming text(body),
   * resulting in a Dataframe containing the various stages of preprocessing for further operations
   *
   * please find a listing of what kinds of data are available after running this method in the github README
   *
   * @param data (key-value structure, build by the method "getTextandID")
   */
  def preprocessArticles(data: DataFrame):DataFrame= {
    //src: https://github.com/JohnSnowLabs/spark-nlp
    //converting the RDD into a dataframe
    //by using the DocumentAssembler we ensure the input data to have the right format for further processing

    //val dataDF = spark.createDataFrame(data.collect()).toDF("_id", "text").limit(100)
    val documentDF = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    //splitting the incoming data into sentences
    val sentenceDetector = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")

    //further splitting of the sentences into tokens
    val tokenizer = new Tokenizer()
      .setInputCols("sentence")
      .setOutputCol("token")

    //removing punctuation, numbers and "dirty" characters
    val normalizer = new Normalizer()
      .setInputCols("token")
      .setOutputCol("normalized")
      .setLowercase(false)

    //getting rid of stopword ("der", "die", "eine", ect.)
    val stopWords = StopWordsCleaner.pretrained("stopwords_de", "de")
      .setInputCols("normalized")
      .setOutputCol("StopWordsCleaner")

    //lemmatizing words -> e.g. reducing words to their root / neutral form
    val lemmatized = LemmatizerModel.pretrained(name = "lemma", lang = "de")
      .setInputCols("StopWordsCleaner")
      .setOutputCol("lemmatizer")

    val keywords = new YakeModel()
      .setInputCols("lemmatizer")
      .setOutputCol("keywords_extracted")
      .setMinNGrams(1)
      .setMaxNGrams(1)
      .setNKeywords(5)

    val doc = documentDF.transform(data)

    //performing NER (named entity recognition)
    val pipeline = PretrainedPipeline("entity_recognizer_md", "de")
    val entity_analyse = pipeline.transform(doc)

    val tokens = entity_analyse.select("_id" ,"token")
    val normalize = normalizer.fit(tokens).transform(tokens)
    val cTokens = stopWords.transform(normalize)
    val lemma = lemmatized.transform(cTokens).drop("token")
    val keyword = keywords.transform(lemma)

    //merging NER and preprocessed results into a single Dataframe to be returned
    entity_analyse.join(keyword, Seq("_id"), joinType = "outer"  )


    //entity_analyse.printSchema
    //keyword.select("_id","keywords.result").show(truncate = false)
  }
}
