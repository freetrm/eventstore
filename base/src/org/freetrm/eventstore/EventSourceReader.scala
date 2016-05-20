package org.freetrm.eventstore

import akka.NotUsed
import akka.stream.scaladsl.Source
import spray.json._

import scala.concurrent.Future


trait EventSourceReader {

  /**
    * Consume events until event `lastOffsetToReadTo` or forever if `lastOffsetToReadTo` is None. 
    */
  def streamEvents(topic: Topic,
                   earliestOffsetToReadFrom: Long,
                   lastOffsetToReadTo: Option[Long]): Source[Event, NotUsed]

  /**
    * Consume event notifications forever.
    */
  def streamNotifications: Source[EventNotification, NotUsed]

  /**
    * The surface is the newest event for each key in a book.
    */
  def latestSurface(topic: Topic): Source[EventSourceEvent, NotUsed]
  
  def listTopics: Future[Seq[Topic]]

  def topicExists(topic: Topic): Future[Boolean]

  def maxSeqNo(topic: Topic): Future[Option[Long]]
  
  def maxTxnNo(topic: Topic): Future[Option[Long]]

  def close(): Unit
}
