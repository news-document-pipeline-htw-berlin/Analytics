import com.mongodb.spark.config.ReadConfig
import department.DepartmentMapping.{mapDepartment, mapDepartmentTest, readJson}
import org.apache.spark.sql.{DataFrame, SparkSession}

object App {

  def joinDataFrames(frame1: DataFrame, frame2: DataFrame): DataFrame = {
    frame1.join(frame2, Seq("_id"), "left_anti")
  }

  def main(args: Array[String]): Unit = {

    var articlesLeft = true;
    val inputUri = DBConnector.createUri("127.0.0.1", "Articles_nlp", "articles_raw")
    val outputUri = DBConnector.createUri("127.0.0.1", "Articles_nlp", "articles_analytics")

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("analysis")
      .config("spark.mongodb.input.uri", inputUri)
      .config("spark.mongodb.output.uri", outputUri)
      .getOrCreate()

    while (articlesLeft) {
      val readConfigInput = DBConnector.createReadConfig(inputUri, spark)
      val mongoDataCrawler = DBConnector.readFromDB(sparkSession = spark, readConfig = readConfigInput)
      val writeConfig = DBConnector.createWriteConfig(outputUri, sparkSession = spark, mode = "append")
      val preprocessor = new Preprocessor()
      val ordersReadConfig = ReadConfig(Map("collection" -> "articles_analytics"), Some(ReadConfig(spark)))
      val mongoDataAnalysis = DBConnector.readFromDBAnalyst(sparkSession = spark, readConfig = ordersReadConfig)
      var new_data: DataFrame = null
      if (mongoDataAnalysis.head(1).isEmpty) {
        new_data = mongoDataCrawler.limit(1)
      } else {
        new_data = joinDataFrames(mongoDataCrawler, mongoDataAnalysis.select("_id")).limit(500)
      }
      if (new_data.count() != 0) {
        val sentimentAnalysis = new SentimentAnalysis(spark)
        val data_sentimentAnalysis = sentimentAnalysis.analyseSentence(preprocessor.run_pp(new_data))
        val data_department =mapDepartmentTest(readJson("src/main/resources/departments.json", spark), data_sentimentAnalysis, spark)
        val textSum = TextSumFromFullArticle.getData(data_department,spark)
        DBConnector.writeToDB(textSum, writeConfig)
      } else {
        articlesLeft = false
      }
    }
  }
}
