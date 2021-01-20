import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DBConnector {

  /*
     Returns a string used for connection to MongoDB
  */

  def createUri(
                 serverAddress: String,
                 db: String,
                 collection: String): String = {
    "mongodb://" + serverAddress + "/" + db + "." + collection
  }

  /*
    Creates a config for reading a MongoDB
   */

  def createReadConfig(inputUri: String, sparkSession: SparkSession): ReadConfig = {
    ReadConfig(Map("spark.mongodb.input.uri" -> inputUri, "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sparkSession)))
  }

  /*
    Creates a config for writing in db, standard mode is overwrite, meaning DB gets dumped and rewritten as given Dataframe
   */

  def createWriteConfig(outputUri: String, replaceDocument: String = "false", mode: String = "overwrite", sparkSession: SparkSession): WriteConfig = {
    WriteConfig(Map(
      "spark.mongodb.output.uri" -> outputUri,
      "replaceDocument" -> replaceDocument,
      "mode" -> mode),
      Some(WriteConfig(sparkSession))
    )
  }

  /*
    Reads the MongoDB specified in readConfig and returns it as RDD[Row]
   */

  def readFromDB(sparkSession: SparkSession, readConfig: ReadConfig): DataFrame = {
    val df = MongoSpark.load(sparkSession, readConfig)
    if (df.head(1).isEmpty) {
      throw new IllegalArgumentException("Empty DB")
    }
    df
  }

  def readFromDBAnalyst(sparkSession: SparkSession, readConfig: ReadConfig): DataFrame = {
    MongoSpark.load(sparkSession, readConfig)
  }

  /*
    Saves a dataframe to db specified in writeConfig
   */

  def writeToDB(savedInstance: DataFrame, writeConfig: WriteConfig): Unit = {
    val columnName = Seq(
      "_id",
      "text",
      "entities",
      "lemmatizer",
      "sentiments",
      "keywords_extracted",
      "authors", "crawl_time",
      "long_url",
      "short_url",
      "news_site",
      "title",
      "description",
      "intro",
      "keywords",
      "published_time",
      "image_links",
      "links",
      "read_time",
      "department"
    )

    val newDf = savedInstance
      .select(
        "_id",
        "text",
        "entities.result",
        "lemmatizer.result",
        "sentimens",
        "keywords_extracted.result",
        "authors",
        "crawl_time",
        "long_url",
        "short_url",
        "news_site",
        "title",
        "description",
        "intro",
        "keywords",
        "published_time",
        "image_links",
        "links",
        "read_time",
        "department"
      ).toDF(columnName: _*)

    MongoSpark.save(newDf, writeConfig)
  }
}
