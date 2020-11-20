import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.SentenceDetector
import com.johnsnowlabs.nlp.annotators._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.util.io.ResourceHelper.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row


class Preprocessor {

  /** this method will run the preprocessing pipeline
   *
   * @param data = crawled article
   */
  def run_pp(data: RDD[Row]) = {
    // Zieht sich ein Datensatz aus der Row.
    val f=data.first()
    val daters = data.filter(x => x==f )
    val textsList = getTextAndAssociatedID(daters)
    preprocessArticles(textsList)
  }

  /** helper method, extracting text body and associated ID
   *
   * @param data = crawled article(s)
   * @return key-value pairs ID -> text-body
   */
  def getTextAndAssociatedID(data: RDD[Row]): RDD[(String, String)] = {
    data.map(x => (x.get(0).toString, x.getString(15)))
  }

  /** this method will perform several operations of preprocessing on the incoming text(body),
   * resulting in a Dataframe containing the various stages of preprocessing for further operations
   *
   * please find a listing of what kinds of data are available after running this method in the github README
   *
   * @param data (key-value structure, build by the method "getTextandID")
   */
  def preprocessArticles(data: RDD[(String, String)]): Unit = {
    //converting the RDD into a dataframe
    val dataDF = spark.createDataFrame(data.collect()).toDF("_id", "text")

    //src: https://github.com/JohnSnowLabs/spark-nlp
    //by using the DocumentAssembler we ensure the input data to have the right format for further processing
    val documentDF = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    //splitting the incoming data into sentences
    val sentenceDetector = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")

    //further splitting of the sentences into tokens
    val tokenizer = new Tokenizer()
      .setInputCols(Array("sentence"))
      .setOutputCol("token")

    //removing punctuation, numbers and "dirty" characters
    val normalizer = new Normalizer()
      .setInputCols(Array("token"))
      .setOutputCol("normalized")
      .setLowercase(false)

    //getting rid of stopword ("der", "die", "eine", ect.)
    val stopWords = StopWordsCleaner.pretrained("stopwords_de", "de")
      .setInputCols("normalized")
      .setOutputCol("StopWordsCleaner")

    //lemmatizing words -> e.g. reducing words to their root / neutral form
    val lemmatized = LemmatizerModel.pretrained(name = "lemma", lang = "de")
      .setInputCols(Array("StopWordsCleaner"))
      .setOutputCol("lemmatizer")

    val doc = documentDF.transform(dataDF)

    //performing NER
    val pipeline = PretrainedPipeline("entity_recognizer_md", "de")
    val entity_analyse = pipeline.transform(doc)

    val tokens = entity_analyse.select("_id" ,"token")
    val normalize = normalizer.fit(tokens).transform(tokens)
    val cTokens = stopWords.transform(normalize)
    val lemma = lemmatized.transform(cTokens).drop("token")

    //merging NER and preprocessing results into a single Dataframe to be returned
    val preprocessData = entity_analyse.join(lemma, Seq("_id"), joinType = "outer"  )
  }
}
