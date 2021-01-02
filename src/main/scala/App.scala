import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.johnsnowlabs.nlp.SparkNLP
import com.johnsnowlabs.nlp.util.io.ResourceHelper.spark
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{col, schema_of_json}
import org.apache.spark.sql.types.{DoubleType, FloatType, StructField}

object App {


  def joinDataFrames(frame1: DataFrame, frame2: DataFrame): DataFrame = {
    frame1.join(frame2, Seq("_id"))
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
    val readConfigOutput = DBConnector.createReadConfig(outputUri, spark)
    val mongoDataCrawler = DBConnector.readFromDB(sparkSession = spark, readConfig = readConfigInput)
    val mongoDataAnalysis = DBConnector.readFromDB(sparkSession = spark, readConfig = readConfigOutput)
    //mongoDataCrawler.toDF()
    //val newDATA = mongoDataAnalysis.map(x =>mongoDataCrawler.map(y => y.get(0)) x.get(0) )
    //val size = newDATA.count()

    //val newDATA = mongoDataCrawler.subtract(mongoDataAnalysis)

    val writeConfig = DBConnector.createWriteConfig(outputUri, sparkSession = spark, mode = "append")
    //TODO abgleichen der dDB ob schon der Artikle Prozesiert wurde
    val preprocessor = new Preprocessor()
    val rawData = preprocessor.getRawDataFrame(mongoDataCrawler)
    val data_analysis_id_rdd = mongoDataAnalysis.map(x => (x.get(0).toString, x.get(0).toString))
    val data_analysis_id_df = spark.createDataFrame(data_analysis_id_rdd.collect()).toDF("_id", "_id2")
    val new_data =rawData.join(data_analysis_id_df, Seq("_id"), joinType= "left_semi")
    new_data.show
    //val new_data= rawData.filter(!(col("_id").isin(data_analysis_id_list:_*) or col("_id").isNull)).toDF
    val sentimentAnalysis = new SentimentAnalysis(spark)
    val data_sentimentAnalysis = sentimentAnalysis.analyseSentens(preprocessor.run_pp(new_data))
    DBConnector.writeToDB(data_sentimentAnalysis,writeConfig)
  }
}
