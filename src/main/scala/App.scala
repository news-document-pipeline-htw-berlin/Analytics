import com.mongodb.spark.config.ReadConfig
import department.DepartmentMapping.{mapDepartment, mapDepartmentTest, readJson}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}

object App {

  var old_counter :Long = 0

  def joinDataFrames(frame1: DataFrame, frame2: DataFrame): DataFrame = {
    frame1.join(frame2, Seq("_id"), "left_anti")
  }

  def main(args: Array[String]): Unit = {

    var articlesLeft = true;
    val inputUri = DBConnector.createUri("127.0.0.1", "Articles_nlp", "articles_raw")
    val outputUri = DBConnector.createUri("127.0.0.1", "Articles_nlp", "articles_analytics")

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("analysis")
      .config("spark.mongodb.input.uri", inputUri)
      .config("spark.mongodb.output.uri", outputUri)
      .getOrCreate()


    val hadoopConfig: Configuration = spark.sparkContext.hadoopConfiguration
    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    val preprocessor = new Preprocessor()
    while (articlesLeft) {
      val readConfigInput = DBConnector.createReadConfig(inputUri, spark)
      val mongoDataCrawler = DBConnector.readFromDB(sparkSession = spark, readConfig = readConfigInput)
      val writeConfig = DBConnector.createWriteConfig(outputUri, sparkSession = spark, mode = "append")
      val ordersReadConfig = ReadConfig(Map("collection" -> "articles_analytics"), Some(ReadConfig(spark)))
      val mongoDataAnalysis = DBConnector.readFromDBAnalyst(sparkSession = spark, readConfig = ordersReadConfig)
      var new_data: DataFrame = null
      if (mongoDataAnalysis.head(1).isEmpty) {
        new_data = mongoDataCrawler.limit(1)
      } else {
        new_data = joinDataFrames(mongoDataCrawler, mongoDataAnalysis.select("_id")).limit(1000)
      }
      if (new_data.count() != old_counter) {
        mongoDataAnalysis.unpersist()
        val sentimentAnalysis = new SentimentAnalysis(spark)
        DBConnector.writeToDB(TextSumFromFullArticle.getData(
          mapDepartmentTest(readJson("src/main/resources/departments.json", spark),
            sentimentAnalysis.analyseSentence(preprocessor.run_pp(new_data)), spark), spark), writeConfig)
        old_counter =new_data.count()
      } else {
        articlesLeft = false
      }

    }
  }
}