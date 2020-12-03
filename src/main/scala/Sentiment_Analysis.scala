import org.apache.spark.sql.DataFrame

class Sentiment_Analysis(sentPath: String) {

 val sentiments: Map[String, Double] = fileLoader(sentPath)

 def fileLoader(path: String): Map[String, Double] ={
  val url = getClass.getResource("/" + path).getPath
  val src = scala.io.Source.fromFile(url)
  val iter = src.getLines().toList
  val result = sentimentListToMap(iter)
  src.close()
  result
 }

 def sentimentListToMap(input: List[String]): Map[String,Double] ={
  input
     .map{x => x.replaceAll("\\|[A-Z][A-Z]\\s"," ")}
     .map{x => x.replaceAll(","," ")}
     .map{x => x.split("\\s")}
     .map{x => (x(0),x(1),x.tail.tail)}
     .map{x => (x._2,x._3++List(x._1))}
     .map(x => (x._2.map(y => (x._1,y))))
     .flatten
     .map(x => x.swap)
     .map(x => (x._1,x._2.toDouble))
     .toMap
 }

 def analyseSentens(data: DataFrame): DataFrame ={
  val analyse = new Sentiment_Analysis("AFINN-111.txt")
  analyse.analyseSentens(data)
 }
}
