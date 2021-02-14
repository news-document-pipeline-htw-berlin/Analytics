package department

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object DepartmentMapping {


  /**
   * @param jsonPath path to department.json
   * @return Map with k,v -> Department -> List of words which categorizes department
   */
  def readJson(jsonPath: String, sparkSession: SparkSession): Broadcast[Map[String, List[String]]] = {
    val json_content = scala.io.Source.fromFile(jsonPath)
    val json = json_content.mkString
    json_content.close()

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val departmentMap = mapper.readValue(json, classOf[Map[String, List[String]]])
    sparkSession.sparkContext.broadcast(departmentMap)
  }


  /**
   * @param department the Map from readJson containing k,v -> Department -> List of words which categorizes department
   * @param keyWords   extracted keywords in mongodb for each article
   * @return list of appropriate categories for this keywords
   */
  def mapDepartment(department: Broadcast[Map[String, List[String]]], keyWords: List[String]): List[String] = {
    department.value.map(x => if (x._2.intersect(keyWords).nonEmpty) x._1 else null).filter(_ != null).toList
  }

  def mapDepartmentTest(department: Broadcast[Map[String, List[String]]], keyWords: DataFrame, sparkSession: SparkSession): DataFrame = {
    val dep = department.value.map(x => (x._1, x._2.map(y => y.toLowerCase)))
    val department_rdd = keyWords.select("long_url", "keywords_extracted.result", "keywords").distinct.rdd.map(x => (x.getAs[String](0),
      if (x.get(2) != null) {
        department.value.map(y => if (y._2.intersect(x.getAs[List[String]](2)).nonEmpty) y._1 else null).filter(_ != null).toList
      }
      else {
        dep.map(y => if (
          y._2.intersect(x.getAs[List[String]](1)).nonEmpty) y._1 else null).filter(_ != null).toList
      }))

    val department_df = sparkSession.createDataFrame(department_rdd).toDF("long_url", "department")

    department_df.join(keyWords, Seq("long_url"), joinType = "outer")
  }

}
