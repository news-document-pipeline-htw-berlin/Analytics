import department.DepartmentMapping._
import org.scalatest.FunSuite

class DepartmentTest extends FunSuite {
  val departments: Map[String, List[String]] = readJson("src/main/resources/departments.json")


  test("Read department.json") {
    val expected = Map("Satire" -> List("Wahrheit", "Bei Tom", "Über die Wahrheit"),
      "Umwelt" -> List("Umwelt", "Umweltpolitik", "Verkehr", "Ökologie", "Natur", "Klimawandel", "Klimaschutz", "Klima", "Automobilindustrie", "Fahrberichte", "Elektromobilität", "Fahrrad", "Oldtimer", "Verkehrsrecht / Service", "Führerscheintest"),
      "Politik" -> List("Politik", "Verkehr", "Deutschland", "Europa", "Amerika", "Afrika", "Asien", "Nahost", "Netzpolitik"),
      "Geschichte" -> List("Geschichte"),
      "Wirtschaft" -> List("Netzökonomie", "Ökonomie", "Wirtschaft", "Geld"),
      "Sport" -> List("Sport", "Fussball"),
      "Wissen" -> List("Wissen", "Wissenschaft", "Gesundheit", "Klimawandel", "Psychologie"))
    assert(departments.size === 15)
    assert(departments.getOrElse("Satire", null) === expected.getOrElse("Satire", null))
    assert(departments.getOrElse("Umwelt", null) === expected.getOrElse("Umwelt", null))
    assert(departments.getOrElse("Politik", null) === expected.getOrElse("Politik", null))
    assert(departments.getOrElse("Geschichte", null) === expected.getOrElse("Geschichte", null))
    assert(departments.getOrElse("Wirtschaft", null) === expected.getOrElse("Wirtschaft", null))
    assert(departments.getOrElse("Sport", null) === expected.getOrElse("Sport", null))
    assert(departments.getOrElse("Wissen", null) === expected.getOrElse("Wissen", null))
  }

  test("Test departments for keywords") {
    val keyWords = List("Verkehr", "Geschichte", "Sport")
    val categories = mapDepartment(departments, keyWords).sorted
    val expected = List("Umwelt", "Geschichte", "Sport", "Politik").sorted
    assert(expected === categories)
  }

  test("Another keywords for departments") {
    val keyWords = List("Klimawandel", "Geld", "Bei Tom")
    val categories = mapDepartment(departments, keyWords).sorted
    val expected = List("Wissen", "Umwelt", "Wirtschaft", "Satire").sorted
    assert(expected === categories)
  }

  test("No categories") {
    val keyWords = List("Klausur", "Vorbereiten", "Zwei Tage", "Einfach")
    val categories = mapDepartment(departments, keyWords).sorted
    val expected = List()
    assert(expected === categories)
  }

  // categories not in expected, lookup in departments.json

  test("Many categories") {
    val keyWords = List("Konsum", "Wissen", "Debatte", "Spiele", "Apple", "Gehalt", "Berlin", "Film")
    val categories = mapDepartment(departments, keyWords).sorted
    val expected = List("Gesellschaft", "Wissen", "Meinung", "Panorama", "Kultur", "Regional", "Arbeit", "Digital").sorted
    assert(expected === categories)

  }

}
