import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.johnsnowlabs.nlp.SparkNLP
import com.johnsnowlabs.nlp.util.io.ResourceHelper.spark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{col, schema_of_json}
import org.apache.spark.sql.types.{DoubleType, FloatType, StringType, StructField}

object App {


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
        val data_sentimentAnalysis = sentimentAnalysis.analyseSentens(preprocessor.run_pp(new_data))


        val text_sum_From_Full_Article = new TextSumFromFullArticle(spark)
        //val data_Analysis=simpleTextSum.applyGetTopSentences(data_sentimentAnalysis)
        //data_Analysis.show(false)


        DBConnector.writeToDB(data_sentimentAnalysis, writeConfig)
      } else {
        articlesLeft = false
      }
    }
  }

}
