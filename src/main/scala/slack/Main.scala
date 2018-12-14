package slack

import akka.actor._
import com.databricks._
import com.typesafe.config.ConfigFactory
import slack.rtm.SlackRtmClient

trait Command
case object UnknowCommand extends Command
case class ExecCommand(command: String) extends Command
case object StatusCommand extends Command

object Main extends App {
  val conf = ConfigFactory.load("db")

  val clusterId: String = conf.getString("shard.cluster")
  val shard = Shard(conf).connect
  val result = shard.ec.create("scala", clusterId)
  var contextId = result.id

  val token = conf.getString("slack.token")
  implicit val system = ActorSystem("slack")
  implicit val ec = system.dispatcher

  val client = SlackRtmClient(token)
  val selfId = client.state.self.id

  val map = scala.collection.mutable.Map[String, String]()

  def parseCommand(text: String): Command = {
    val status = """(\S+)\s+status""".r
    val qq = """^(\S+)\s+qq\s+```\s*(.*)\s*```""".r
    text match {
      case status(_) => StatusCommand
      case qq(_, command) => ExecCommand(command)
      case _ => UnknowCommand
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
                val IdResult(commandId) = shard.command.execute(
                  "scala", clusterId, contextId, command)
                map.update("scala", commandId)
                s"Hey <@${message.user}>, got it ..."
              } catch {
                case e: Throwable =>
                  s"Can you do something with that? ${e.getClass.getName}: ${e.getMessage}"
              }
              client.sendMessage(message.channel, answer)
            case StatusCommand =>
              val answer = map.get("scala") match {
                case None => "I have nothing to say to you"
                case Some(commandId) =>
                  try {
                    val res = shard.command.status(clusterId, contextId, commandId)
                    res.status + Option(res.results.data).map(d => s"""\n```${d}```""").getOrElse("")
                  } catch {
                    case e: Throwable =>
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
