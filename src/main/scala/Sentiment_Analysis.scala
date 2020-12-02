import org.apache.spark.sql.DataFrame

class Sentiment_Analysis(sentPath: String) {

 val sentiments: Map[String, Int] = fileLoader(sentPath)

 def fileLoader(path: String): Map[String, Int] ={
  val url = getClass.getResource("/" + path).getPath
  val src = scala.io.Source.fromFile(url)
  val iter = src.getLines()
  val result: Map[String, Int] = (for (row <- iter) yield {
   val seg = row.split("\t"); (seg(0) -> seg(1).toInt)
  }).toMap
  src.close()
  result
 }

 def analyseSentens(data: DataFrame): DataFrame ={
  val analyse = new Sentiment_Analysis("AFINN-111.txt")
  analyse.analyseSentens(data)
 }
}
