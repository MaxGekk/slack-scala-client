package slack

import org.scalatest.FunSuite
import Main._

class CommandParsingSuite extends FunSuite {
  test("parse status") {
    val text =
      s"""<@UESRZGZSQ> status""".stripMargin
    val command = parseCommand(text)
    assert(command == StatusCommand("scala"))
  }

  test("parse status - python") {
    val text =
      s"""<@UESRZGZSQ> status python""".stripMargin
    val command = parseCommand(text)
    assert(command == StatusCommand("python"))
  }

  test("parse short command") {
    val text =
      s"""<@UESRZGZSQ> qq `spark.sql("show databases").show()`"""
    val command = parseCommand(text)
    assert(command == ExecCommand("""spark.sql("show databases").show()""", "scala"))
  }

  test("parse short command - python") {
    val text =
      s"""<@UESRZGZSQ> python `spark.sql('show databases').show()`"""
    val command = parseCommand(text)
    assert(command == ExecCommand("""spark.sql('show databases').show()""", "python"))
  }

  test("parse command") {
    val text =
      s"""<@UESRZGZSQ> qq ```spark.sql("show databases").show()```"""
    val command = parseCommand(text)
    assert(command == ExecCommand("""spark.sql("show databases").show()""", "scala"))
  }

  test("parse command - r") {
    val text =
      s"""<@UESRZGZSQ> r ```1```"""
    val command = parseCommand(text)
    assert(command == ExecCommand("""1""", "r"))
  }

  test("parse multiLine qq") {
    val text =
      s"""<@UESRZGZSQ> qq
         |```
         |val databases = spark.sql("show databases")
         |databases.show()
         |```""".stripMargin
    val command = parseCommand(text)
    assert(command == ExecCommand(
      """
        |val databases = spark.sql("show databases")
        |databases.show()
        |""".stripMargin, "scala"))
  }

  test("parse multiLine sql") {
    val text =
      s"""<@UESRZGZSQ> sql
         |```show databases```""".stripMargin
    val command = parseCommand(text)
    assert(command == ExecCommand(s"""show databases""", "sql"))
  }

  test("parse multiLine qq command") {
    val text =
      s"""<@UESRZGZSQ> qq
         |```
         |spark.sql("show databases").show()
         |```""".stripMargin
    val command = parseCommand(text)
    assert(command == ExecCommand(
      s"""
         |spark.sql("show databases").show()
         |""".stripMargin, "scala"))
  }

  test("reset") {
    val text =
      s"""<@UESRZGZSQ> reset""".stripMargin
    val command = parseCommand(text)
    assert(command == ResetCommand)
  }

  test("cancel") {
    val text = """<@UESRZGZSQ> cancel"""
    val command = parseCommand(text)
    assert(command == CancelCommand("scala"))
  }

  test("cancel - r") {
    val text = """<@UESRZGZSQ> cancel r"""
    val command = parseCommand(text)
    assert(command == CancelCommand("r"))
  }

  test("cut html") {
    val res = Main.cutHtml("""<pre style="font-size:10p"></pre><pre style = 'font-size:10pt'>[1] 1</pre>""")
    assert(res == "```[1] 1```")
  }
}
