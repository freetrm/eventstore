package org.freetrm.eventstore.db

import java.sql.Timestamp

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import org.freetrm.eventstore._
import slick.driver.JdbcProfile

import scala.concurrent.Future

trait DBTopicReader {
  driver: JdbcProfile =>

  implicit val system: ActorSystem
  import system.dispatcher
  
  protected val tables = new Tables(driver)

  import api._

  protected def db: driver.backend.DatabaseDef

  protected val events = TableQuery[tables.EventsSchema]

  protected val maxFetch: Int = 1000
  
  def close() = db.close()

  def fetch(topic: Topic,
            earliestOffsetToReadFrom: Long,
            lastOffsetToReadTo: Option[Long]): Future[Vector[Event]] = {

    val res = for {
      rows <- fetchQuery(topic, earliestOffsetToReadFrom, lastOffsetToReadTo)
      res = rows.toVector.map(resultSetToEvent)
    } yield res

    db.run(res)
  }

  def latestSurface(topic: Topic): Source[EventSourceEvent, NotUsed] = {
    Source.fromPublisher(
      db.stream(events.sortBy(_.sequenceNumber.desc).result)
    ).map(
      resultSetToEvent
    ).statefulMapConcat(() => {
      var keys = Map[String, String]()
      x => x match {
        case EventSourceEvent(EventVersionPair(seqNo, _), key, hash, data) =>
          if (!keys.contains(key)) {
            keys += key -> hash
            // this is the surface of the EventStore so just sending data as transactions don't make a lot of sense.
            EventSourceEvent(EventVersionPair(seqNo, 0), key, hash, data) :: Nil
          } else {
            Nil
          }
        case e => Nil
          
      }
    })
  }
  
  private def resultSetToEvent(rs: (String, Long, String, String, String, Timestamp, Long, Boolean)) = {
     rs match {
       case (_, seqNo, _, _, data, _, txnNo, true /*isMetadata*/ ) if data == Tables.TxnStartData =>
         EventTransactionStart(EventVersionPair(seqNo, txnNo))

       case (_, seqNo, _, _, data, _, txnNo, true /*isMetadata*/ ) if data == Tables.TxnEndData =>
         EventTransactionEnd(EventVersionPair(seqNo, txnNo))

       case (_, seqNo, eventID, contentHash, data, _, txnNo, true /*isMetadata*/ ) =>
         throw new Exception(s"Invalid metadata in table: $data")

       case (_, seqNo, eventID, contentHash, data, _, txnNo, _) =>
         EventSourceEvent(EventVersionPair(seqNo, txnNo), eventID, contentHash, data)
     }
  }

  private def fetchQuery(topic: Topic,
                         earliestOffsetToReadFrom: Long,
                         lastOffsetToReadTo: Option[Long]) = {

    val limit = lastOffsetToReadTo.fold(maxFetch)(last => (last - earliestOffsetToReadFrom + 1).toInt.min(maxFetch))
    require(limit > 0, s"Invalid limit, params: $earliestOffsetToReadFrom, $lastOffsetToReadTo")

    for {
      rows <- events.filter(
        e => e.topic === topic.name && e.sequenceNumber >= earliestOffsetToReadFrom
      ).sortBy(_.sequenceNumber).take(limit).result
    } yield rows
  }
}

case class H2DBReader(dbUrl: String, user: String = null, password: String = null, override val maxFetch: Int = 1000)
                     (implicit val system: ActorSystem) 
  extends DBTopicReader with slick.driver.H2Driver {

  Class.forName("org.h2.Driver")

  import api._

  override val db = Database.forURL(dbUrl)
}

case class SqlServerDBReader(dbUrl: String, user: String = null, password: String = null,
                             override val maxFetch: Int = 1000)
                            (implicit val system: ActorSystem)
  extends DBTopicReader with freeslick.MSSQLServerProfile {

  import api._

  override val db = Database.forURL(dbUrl)
}
