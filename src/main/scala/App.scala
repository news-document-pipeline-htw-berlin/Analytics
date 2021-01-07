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

    val inputUri = DBConnector.createUri("127.0.0.1", "Articles_nlp", "articles_raw")
    val outputUri = DBConnector.createUri("127.0.0.1", "Articles_nlp", "articles_analytics")

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("analysis")
      .config("spark.mongodb.input.uri", inputUri)
      .config("spark.mongodb.output.uri", outputUri)
      .getOrCreate()

    val readConfigInput = DBConnector.createReadConfig(inputUri, spark)
    val mongoDataCrawler = DBConnector.readFromDB(sparkSession = spark, readConfig = readConfigInput).limit(501)
    val ordersReadConfig = ReadConfig(Map("collection"->"articles_analytics"), Some(ReadConfig(spark)))
    val mongoDataAnalysis = DBConnector.readFromDB(sparkSession = spark, readConfig = ordersReadConfig).select("_id")
    val writeConfig = DBConnector.createWriteConfig(outputUri, sparkSession = spark, mode = "append")
    val preprocessor = new Preprocessor()
    val new_data = joinDataFrames(mongoDataCrawler, mongoDataAnalysis)
    val sentimentAnalysis = new SentimentAnalysis(spark)
    val data_sentimentAnalysis = sentimentAnalysis.analyseSentens(preprocessor.run_pp(new_data))

    val simpleTextSum = new SimpleTextSum(spark)
    //val data_Analysis=simpleTextSum.applyGetTopSentences(data_sentimentAnalysis)
    //data_Analysis.show(false)

    DBConnector.writeToDB(data_sentimentAnalysis, writeConfig)

  }
}
