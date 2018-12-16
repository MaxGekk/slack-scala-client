package slack

import org.scalatest.FunSuite
import com.databricks.ApiTableResult

class ShowTableTests extends FunSuite {
  test("simple table") {
    val table = ApiTableResult(
      data = List(List("Leo", "Patterson"), List("Thomas", "Robinson")),
      schema = List(Map("name" -> "col0"), Map("name" -> "col1")),
      truncated = false,
      isJsonSchema = true)
    println(Main.showTable(table, 10))
  }
}
