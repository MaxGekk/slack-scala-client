package slack

import org.scalatest.FunSuite
import Main._

class CommandParsingSuite extends FunSuite {
  test("parse status") {
    val text =
      s"""<@UESRZGZSQ> status""".stripMargin
    val command = parseCommand(text)
    assert(command == StatusCommand)
  }

  test("parse command 1") {
    val text =
      s"""<@UESRZGZSQ> qq ```spark.sql("show databases").show()```""".stripMargin
    val command = parseCommand(text)
    assert(command == ExecCommand("""spark.sql("show databases").show()"""))
  }

  test("parse multiLine qq") {
    val text =
      s"""<@UESRZGZSQ> qq
         |```spark.sql("show databases").show()```""".stripMargin
    val command = parseCommand(text)
    assert(command == ExecCommand(s"""spark.sql("show databases").show()"""))
  }

  test("parse multiLine qq command") {
    val text =
      s"""<@UESRZGZSQ> qq
         |```
         |spark.sql("show databases").show()
         |```""".stripMargin
    val command = parseCommand(text)
    assert(command == ExecCommand(s"""spark.sql("show databases").show()"""))
  }
}
