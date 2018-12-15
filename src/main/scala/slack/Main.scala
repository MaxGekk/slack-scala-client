package slack

import akka.actor._
import com.databricks._
import com.typesafe.config.ConfigFactory
import slack.rtm.SlackRtmClient

trait Command
case object UnknowCommand extends Command
case class ExecCommand(command: String) extends Command
case object StatusCommand extends Command
case object CancelCommand extends Command
case object ResetCommand extends Command
case object HelpCommand extends Command

case class ExecutionContext(id: String, lastCommandId: Option[String])

object Main extends App {
  val conf = ConfigFactory.load("db")

  val clusterId: String = conf.getString("shard.cluster")
  val shard = Shard(conf).connect
  val lang = "scala"
  
  val token = conf.getString("slack.token")
  implicit val system = ActorSystem("slack")
  implicit val ec = system.dispatcher

  val client = SlackRtmClient(token)
  val selfId = client.state.self.id

  val contexts = scala.collection.mutable.Map[String, ExecutionContext]()

  def parseCommand(text: String): Command = {
    val status = """(\S+)\s+status""".r
    val qq = """^(\S+)\s+qq\s+```\s*(.*)\s*```""".r
    val qqShort = """^(\S+)\s+qq\s+`(.*)`""".r
    val reset = """(\S+)\s+reset""".r
    val cancel = """(\S+)\s+cancel""".r
    val help = """(\S+)\s+help""".r

    text match {
      case status(_) => StatusCommand
      case qq(_, command) => ExecCommand(command)
      case qqShort(_, command) => ExecCommand(command)
      case reset(_) => ResetCommand
      case cancel(_) => CancelCommand
      case help(_) => HelpCommand
      case _ => UnknowCommand
    }
  }

  def getContext(language: String): ExecutionContext = {
    contexts.get(language) match {
      case None =>
        val result = shard.ec.create(language, clusterId)
        val id = result.id
        val context = ExecutionContext(id, None)
        contexts.update(language, ExecutionContext(id, None))
        context
      case Some(executionContext) => executionContext
    }
  }

  def destroyContext(language: String): Unit = {
    contexts.get(language).foreach { context =>
      try {
        shard.ec.destroy(clusterId, context.id)
      } catch {
        case e: Exception =>
          system.log.info(s"Tried to remove execution context but failed: ${e.toString}")
      } finally {
        contexts.remove(language)
      }
    }
  }

  def cancel(language: String): String = {
    contexts.get(language) match {
      case None => s"Nothing to cancel for ${language}"
      case Some(ExecutionContext(_, None)) => "Run a command before cancel it"
      case Some(ExecutionContext(contextId, Some(commandId))) =>
        try {
          shard.command.cancel(clusterId, contextId, commandId)
          s"The last command for ${language} has been canceled"
        } catch {
          case e: Exception =>
            s"I got the exception ${e.getClass.getName}: ${e.getMessage}"
        }
    }
  }

  def checkStatus(channel: String): Unit = {
    val answer = contexts.get(lang) match {
      case None => "I don't any clue what you want."
      case Some(ExecutionContext(_, None)) => "What's up?"
      case Some(ExecutionContext(contextId, Some(commandId))) =>
        try {
          val res = shard.command.status(clusterId, contextId, commandId)
          res match {
            case CommandResult(_, status, ApiTextResult(data)) =>
              status + "\n" + s"```${data}```"
            case CommandResult(_, status, ApiErrorResult(None, cause)) =>
              s"""$status
                 |${cause.take(500) + " ..."}
               """.stripMargin
            case CommandResult(_, status, ApiErrorResult(Some(summary), cause)) =>
              s"""$status
                 |$summary
                 |${cause.take(500) + " ..."}
               """.stripMargin
            case CommandResult(_, status, ApiImageResult(fileName)) =>
              s"""$status
                 |image = ${fileName}
               """.stripMargin
            case CommandResult(_, status, table: ApiTableResult) =>
              s"""$status
                 |I received the table (truncated=${table.truncated}, isJsonSchema=${table.isJsonSchema})
                 |${table.schema.toString()}
                 |
                 |${table.data.map(_.mkString(",")).mkString("\n")}
               """.stripMargin
            case CommandResult(_, status, _) => status
            case unknown => s"Unknown result = ${unknown.toString}"
          }
        } catch {
          case e: Exception =>
            s"""Oops, I got the exception:
               | contextId: ${contextId} commandId: ${commandId}
               |${e.getClass.getName}: ${e.getStackTrace.mkString("\n")}""".stripMargin
        }
    }
    client.sendMessage(channel, answer)
  }

  client.onEvent { event =>
    system.log.info("Received new event: {}", event)
    import models._
    event match {
      case message: Message =>
        val mentionedIds = SlackUtil.extractMentionedIds(message.text)
        if (mentionedIds.contains(selfId)) {
          val command = parseCommand(message.text)
          command match {
            case ExecCommand(commandText) =>
              client.indicateTyping(message.channel)
              val answer = try {
                val ec = getContext(lang)
                val IdResult(commandId) = shard.command.execute(lang, clusterId, ec.id, commandText)
                contexts.update(lang, ec.copy(lastCommandId = Some(commandId)))
                s"Hey <@${message.user}>, got it ..."
              } catch {
                case e: Exception =>
                  destroyContext(lang)
                  s"Can you do something with that? ${e.getClass.getName}: ${e.getMessage}"
              }
              client.sendMessage(message.channel, answer)
              client.indicateTyping(message.channel)
              Thread.sleep(1000)
              checkStatus(message.channel)
            case StatusCommand =>
              client.indicateTyping(message.channel)
              checkStatus(message.channel)
            case ResetCommand =>
              client.indicateTyping(message.channel)
              destroyContext(lang)
              client.sendMessage(message.channel, s"Done, new context: ${getContext(lang)}")
            case CancelCommand =>
              client.indicateTyping(message.channel)
              client.sendMessage(message.channel, cancel(lang))
            case HelpCommand => client.sendMessage(message.channel, printHelp(message.user))
            case _ =>
              client.sendMessage(message.channel, "What do you mean?")
          }
        }
      case others => system.log.info(others.toString)
    }
  }

  def printHelp(from: String): String = {
    val bot = "@db"
   s"""Welcome to my hugs, <@${from}>. Here is what I can do:
      |- Answer to your quick questions: *$bot qq `1+1`*. I'll run it in Scala Repl.
      |- Run Scala code:
      |*$bot qq*
      |```
      |spark.range(10).count
      |```
      |- Show status of last command: *$bot status*
      |- Canceling a command like Thread.sleep(100000): *$bot cancel*
      |- If something goes wrong, just reset me: *$bot reset*
    """.stripMargin
  }
}
