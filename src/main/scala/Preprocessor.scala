import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.SentenceDetector
import com.johnsnowlabs.nlp.annotators.{Lemmatizer, LemmatizerModel, Normalizer, Stemmer, StopWordsCleaner, Tokenizer}
import com.johnsnowlabs.nlp.util.io.ResourceHelper.spark
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row


class Preprocessor {
  //TODO Prüfung ob er Text schon  Processiert wurde

  /**
   *
   * @param data
   */
  def run_pp(data: RDD[Row]) = {
    // Zieht sich ein Daten Satz aus der Row.
    val f=data.first()
    val daters = data.filter(x => x==f )
    val textsList = getTextandID(daters)
    sentenceTokenizer(textsList)
  }

  /**
   *
   * @param data
   * @return
   */
  def getTextandID(data: RDD[Row]): RDD[(String, String)] = {
    data.map(x => (x.get(0).toString, x.getString(15)))
  }

  /**
   *
   * @param data
   */
  def sentenceTokenizer(data: RDD[(String, String)]): Unit = {
    val dataDF = spark.createDataFrame(data.collect()).toDF("_id", "text")

    val documentDF = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val sentenceDetector = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")

    val tokenizer = new Tokenizer()
      .setInputCols(Array("sentence"))
      .setOutputCol("token")

    val normalizer = new Normalizer()
      .setInputCols(Array("token"))
      .setOutputCol("normalized")
      .setLowercase(false)

    val stopWords = StopWordsCleaner.pretrained("stopwords_de", "de")
      .setInputCols("normalized")
      .setOutputCol("StopWordsCleaner")

    val lemmer = LemmatizerModel.pretrained(name = "lemma", lang = "de")
      .setInputCols(Array("StopWordsCleaner"))
      .setOutputCol("lemmatizer")

    val stemmer = new Stemmer()
      .setInputCols("StopWordsCleaner")
      .setOutputCol("stemmer")
      .setLanguage("German")

    val doce = documentDF.transform(dataDF)
    val pipeline = PretrainedPipeline("entity_recognizer_md", "de")
    val entity_analyse  =pipeline.transform(doce)



    val tokens = entity_analyse.select("_id" ,"token")
    val normalize = normalizer.fit(tokens).transform(tokens)
    val cTokens = stopWords.transform(normalize)
    val lemma = lemmer.transform(cTokens).drop("token")
    val preprocessData = entity_analyse.join(lemma, Seq("_id"), joinType = "outer"  )

  }

}
