package slack

import akka.actor._
import com.databricks._
import com.typesafe.config.ConfigFactory
import slack.rtm.SlackRtmClient

trait Command
case object UnknowCommand extends Command
case class ExecCommand(command: String) extends Command
case object StatusCommand extends Command

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
    text match {
      case status(_) => StatusCommand
      case qq(_, command) => ExecCommand(command)
      case qqShort(_, command) => ExecCommand(command)
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
      case Some(ec) => ec
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

  client.onEvent { event =>
    system.log.info("Received new event: {}", event)
    import models._
    event match {
      case message: Message => {
        val mentionedIds = SlackUtil.extractMentionedIds(message.text)
        if (mentionedIds.contains(selfId)) {
          val command = parseCommand(message.text)
          command match {
            case ExecCommand(command) =>
              val answer = try {
                val ec = getContext(lang)
                val IdResult(commandId) = shard.command.execute(lang, clusterId, ec.id, command)
                contexts.update(lang, ec.copy(lastCommandId = Some(commandId)))
                s"Hey <@${message.user}>, got it ..."
              } catch {
                case e: Exception =>
                  destroyContext(lang)
                  s"Can you do something with that? ${e.getClass.getName}: ${e.getMessage}"
              }
              client.sendMessage(message.channel, answer)
            case StatusCommand =>
              val answer = contexts.get(lang) match {
                case None => "I don't any clue what you want."
                case Some(ExecutionContext(_, None)) => "What's up?"
                case Some(ExecutionContext(contextId, Some(commandId))) =>
                  try {
                    val res = shard.command.status(clusterId, contextId, commandId)
                    res.status + Option(res.results.data).map(d => s"""\n```${d}```""").getOrElse("")
                  } catch {
                    case e: Exception =>
                      destroyContext(lang)
                      s"Oops, I got the exception: ${e.getClass.getName}: ${e.getMessage}"
                  }
              }
              client.sendMessage(message.channel, answer)
            case _ =>
              client.sendMessage(message.channel, "What do you mean?")
          }
        }
      }
      case others => system.log.info(others.toString)
    }
  }
}
