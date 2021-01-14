import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.ObjectMapper

class DepartmentMapping {


  def readJson(jsonPath: String): Map[String, List[String]] = {
    val json_content = scala.io.Source.fromFile(jsonPath)
    val json = json_content.mkString
    json_content.close()
    val mapper = new ObjectMapper()

    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(json, classOf[Map[String, List[String]]])
  }


  def mapDepartment(json: Map[String, List[String]], keyWords: List[String]): List[String] = {
    json.map(x => if (x._2.intersect(keyWords).nonEmpty) x._1 else null).filter(_ != null).toList
  }

}
