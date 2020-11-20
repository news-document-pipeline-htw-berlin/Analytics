import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.johnsnowlabs.nlp.SparkNLP
object App {



  def joinDataFrames(frame1: DataFrame, frame2: DataFrame): DataFrame = {
    frame1.join(frame2, Seq("_id"))
  }


  def main(args: Array[String]): Unit = {

    val inputUri = DBConnector.createUri("127.0.0.1", "Articles_nlp", "articles")
    val outputUri = DBConnector.createUri("127.0.0.1", "Articles_nlp", "articles")

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Author analysis")
      .config("spark.mongodb.input.uri", inputUri)
      .config("spark.mongodb.output.uri", outputUri)
      .getOrCreate()

    val readConfig = DBConnector.createReadConfig(inputUri, spark)
    val writeConfig = DBConnector.createWriteConfig(outputUri, sparkSession = spark)
    val mongoData = DBConnector.readFromDB(sparkSession = spark, readConfig = readConfig)
    val preprocessor = new Preprocessor()
    preprocessor.run_pp(mongoData)

  }
}
