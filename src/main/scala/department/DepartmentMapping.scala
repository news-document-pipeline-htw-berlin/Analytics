package department

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

object DepartmentMapping {


  /**
   * @param jsonPath path to department.json
   * @return Map with k,v -> Department -> List of words which categorizes department
   */
  def readJson(jsonPath: String,sparkSession: SparkSession): Broadcast[Map[String, List[String]]] = {
    val json_content = scala.io.Source.fromFile(jsonPath)
    val json = json_content.mkString
    json_content.close()

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val departmentMap =  mapper.readValue(json, classOf[Map[String, List[String]]])
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

}
