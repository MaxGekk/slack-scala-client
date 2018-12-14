package slack

import akka.actor._
import com.databricks._
import com.typesafe.config.ConfigFactory
import slack.rtm.SlackRtmClient

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

  client.onEvent { event =>
    system.log.info("Received new event: {}", event)
    import models._
    event match {
      case message: Message => {
        val mentionedIds = SlackUtil.extractMentionedIds(message.text)
        if (mentionedIds.contains(selfId)) {
          val split = message.text.split("\n").toSeq
          system.log.info(split.mkString("=>", ",", "<="))
          split match {
            case Seq(first, command) if first.contains("qq") =>
              val answer = try {
                val IdResult(commandId) = shard.command.execute(
                  "scala", clusterId, contextId, command)
                map.update("scala", commandId)
                s"Hey <@${message.user}>, got it ..."
              } catch {
                case e: Throwable =>
                  s"I got the exception: ${e.toString}"
              }
              client.sendMessage(message.channel, answer)
            case Seq(first) if first.contains("status") =>
              val answer = map.get("scala") match {
                case None => "What's up?"
                case Some(commandId) =>
                  try {
                    val res = shard.command.status(clusterId, contextId, commandId)
                    system.log.info("res = " + res)
                    res.status + Option(res.results.data).map("\n" + _).getOrElse("")
                  } catch {
                    case e: Throwable =>
                      s"I got the exception: ${e.toString}"
                  }
              }
              client.sendMessage(message.channel, answer)
            case Seq(first) if first.contains("restart") =>
              client.sendMessage(message.channel, s"Sure <@${message.user}>")
              try {
                shard.ec.destroy(clusterId, contextId)
              } catch {
                case e: Throwable =>
                  client.sendMessage(message.channel, s"I got the exception: ${e.toString}")
              }
              val result = shard.ec.create("scala", clusterId)
              contextId = result.id
            case unknown => system.log.info(unknown.toString)
          }
        }
      }
      case others => system.log.info(others.toString)
    }
  }
}
