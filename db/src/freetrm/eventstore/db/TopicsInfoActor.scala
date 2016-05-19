package freetrm.eventstore.db

import akka.actor.{ActorRef, ActorLogging, Actor}
import freetrm.eventstore.{Topic, EventVersionPair}
import freetrm.eventstore.db.TopicsInfoActor._

import scala.collection.mutable


/**
  * An Actor which contains the offets of all the topics. You can subscribe to this actor
  * to get updates when topics have new Events.
  * 
  * When the actor shuts down it will tell all the listeners by sending a TopicsInfoActorShutdown
  * message.
  */
class TopicsInfoActor extends Actor with ActorLogging {
  private var offsets: Map[Topic, EventVersionPair] = Map()
  
  // Interested in a specific topic, Some("topicname"), or all topics, None.
  private val listeners: mutable.Map[Option[Topic], Set[ActorRef]] = mutable.Map()

  override def receive = {
    case update@TopicOffsetInfo(topic, version) =>
      offsets += topic -> version
      listeners.getOrElse(Some(topic), Set()).foreach(_ ! update)
      listeners.getOrElse(None, Set()).foreach(_ ! TopicOffsets(offsets))

    case QueryOffsetForTopic(topic) =>
      sender() ! offsets.get(topic)

    case QueryOffsets =>
      sender() ! TopicOffsets(offsets)

    case RegisterInterest(filterTopic) =>
      val l = listeners.getOrElse(filterTopic, Set())
      listeners.put(filterTopic, l + sender())
      filterTopic match {
        case Some(t) =>
          offsets.get(t).foreach {
            case info => sender() ! info
          }
        case _ =>
          sender() ! TopicOffsets(offsets)
      }

    case UnregisterInterest(filterTopic) =>
      val l = listeners.getOrElse(filterTopic, Set())
      listeners.put(filterTopic, l - sender())

    case shutdown@TopicsInfoActorShutdown(reason) =>
      listeners.values.foreach {
        _.foreach(_ ! shutdown)
      }
      context.stop(self)
  }
}

object TopicsInfoActor {
  case class TopicOffsetInfo(topic: Topic, version: EventVersionPair)

  case class RegisterInterest(filterTopic: Option[Topic])

  case class UnregisterInterest(filterTopic: Option[Topic])
  
  case class QueryOffsetForTopic(topic: Topic)
  
  case object QueryOffsets
  
  case class TopicOffsets(offsets: Map[Topic, EventVersionPair])
  
  case class TopicsInfoActorShutdown(reason: String)
}
