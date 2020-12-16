import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.johnsnowlabs.nlp.SparkNLP
import org.apache.spark.sql.functions.{col, schema_of_json}
import org.apache.spark.sql.types.{DoubleType, FloatType, StructField}

object App {


  def joinDataFrames(frame1: DataFrame, frame2: DataFrame): DataFrame = {
    frame1.join(frame2, Seq("_id"))
  }


  def main(args: Array[String]): Unit = {

    val inputUri = DBConnector.createUri("127.0.0.1", "Articles_nlp", "articles")
    val outputUri = DBConnector.createUri("127.0.0.1", "Articles_nlp", "articles_analytics")

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("analysis")
      .config("spark.mongodb.input.uri", inputUri)
      .config("spark.mongodb.output.uri", outputUri)
      .getOrCreate()

    val readConfigInput = DBConnector.createReadConfig(inputUri, spark)
    val readConfigOutput = DBConnector.createReadConfig(outputUri, spark)
    val mongoDataCrawler = DBConnector.readFromDB(sparkSession = spark, readConfig = readConfigInput)
    val mongoDataAnalysis = DBConnector.readFromDB(sparkSession = spark, readConfig = readConfigOutput)
    mongoDataCrawler.toDF()
    val newDATA = mongoDataAnalysis.map(x =>mongoDataCrawler.map(y => y.get(0)) x.get(0) )
    val size = newDATA.count()
    val writeConfig = DBConnector.createWriteConfig(outputUri, sparkSession = spark, mode = "append")
    //TODO abgleichen der dDB ob schon der Artikle Prozesiert wurde
    val preprocessor = new Preprocessor()
    val sentimentAnalysis = new SentimentAnalysis(spark)
    val data_sentimentAnalysis = sentimentAnalysis.analyseSentens(preprocessor.run_pp(newDATA))
    data_sentimentAnalysis.s
    val keywords = new Key_Words()
   //keywords.tf_idf(data_sentimentAnalysis)
    DBConnector.writeToDB(data_sentimentAnalysis,writeConfig)
  }
}
