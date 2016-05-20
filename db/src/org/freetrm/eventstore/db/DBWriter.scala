package org.freetrm.eventstore.db

import java.sql.{Statement, DriverManager, Connection}
import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArrayList}
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.ActorRef
import akka.util.Timeout
import org.freetrm.eventstore.db.TopicsInfoActor.{QueryOffsetForTopic, TopicOffsetInfo}
import org.freetrm.eventstore._
import org.freetrm.eventstore.utils.Log
import slick.driver.JdbcProfile
import slick.jdbc.TransactionIsolation.Serializable

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}
import scala.util.control.NonFatal
import scala.collection.JavaConversions._
import akka.pattern.ask
import akka.actor._

trait DBWriter extends EventSourceWriter with Log {
  driver: JdbcProfile =>

  implicit val askTimeout = new Timeout(FiniteDuration(2, "s"))

  protected val tables = new Tables(driver)

  import api._

  protected def db: driver.backend.DatabaseDef

  protected val system: ActorSystem

  private val started: AtomicBoolean = new AtomicBoolean(false)
  
  private val versions = new ConcurrentHashMap[Topic, EventVersionPair]()
  private val transactions = new ConcurrentHashMap[Topic, Long]()

  private val events = TableQuery[tables.EventsSchema]

  // Because of https://github.com/slick/slick/issues/214
  private lazy val curtime = SimpleFunction.nullary[java.sql.Timestamp]("current_timestamp")

  protected val topicInfoActor: Option[ActorRef]
  
  def start(): Future[Unit] = {
    require(!started.getAndSet(true), "Already started")

    val query = events.groupBy(_.topic).map {
      case (topic, t) =>
        topic -> (t.map(_.txnId).max, t.map(_.sequenceNumber).max)
    }.result
    
    db.run(query).map {
      case results =>
        results.foreach {
          case (topic, (Some(maxTxn), Some(maxSeq))) =>
            val version = EventVersionPair(maxSeq, maxTxn)
            versions.put(Topic(topic), version)
            topicInfoActor.foreach(_ ! TopicOffsetInfo(Topic(topic), version))
            
          case o =>
            throw new Exception("Bad results from db: " + o)
        }
    }
  }

  def dropAndRecreate(): Future[Boolean] = {
    val run = db.run(events.schema.drop).recover { case _ => Unit }
    
    run.flatMap {
      _ =>
        db.run(
          events.schema.create
        ).map(_ => true)
    }
  }
  
  def versionInfo(topic: Topic): Option[EventVersionPair] = Option(versions.get(topic))
  
  def maxTxnNo(topic: Topic): Option[Long] = versionInfo(topic).map(_.txnNo)
  
  def maxSeqNo(topic: Topic): Option[Long] = versionInfo(topic).map(_.seqNo)
  
  private def checkSeqNo(version: EventVersionPair, topic: Topic) = {
    versionInfo(topic) match {
      case Some(EventVersionPair(currSeq, currTxn)) =>
        require(version.seqNo > currSeq, s"Invalid seq no: ${version.seqNo}, current seq no: $currSeq")
      case _ =>
        require(version.seqNo == 0, s"Invalid seq no: ${version.seqNo} - should be 0")
    }
  }

  override def write(topic: Topic, event: Event): Future[EventVersionPair] = {
    event match {
      case EventTransactionStart(version) => startTransaction(version, topic)
      case EventTransactionEnd(version) => endTransaction(version, topic)
      case EventSourceEvent(version, key, hash, data) => produce(version, topic, key, hash, data)
      case EventInvalidate(version) => throw new Exception("EventInvalidate not implemented yet") 
    }
  }

  def startTransaction(version: EventVersionPair, topic: Topic): Future[EventVersionPair] = {
    require(!transactions.containsKey(topic), "Already in a transaction")
    checkSeqNo(version, topic)

    val txnNo = maxTxnNo(topic).map(_ + 1).getOrElse(0l)
    transactions.put(topic, txnNo)
    doInsert(version, topic, "", "", txnNo, Tables.TxnStartData, isTxnBoundary = true)
  }

  def endTransaction(version: EventVersionPair, topic: Topic): Future[EventVersionPair] = {
    require(transactions.containsKey(topic), "Not in a transaction")
    checkSeqNo(version, topic)
    
    val txnNo = maxTxnNo(topic)
    require(txnNo.isDefined, "Invalid state, no maxTxn defined.")
    require(txnNo.get == transactions.get(topic), "Invalid state, wrong txn number.")
    transactions.remove(topic)
    doInsert(version, topic, "", "", txnNo.get, Tables.TxnEndData, isTxnBoundary = true)
  }

  def produce(version: EventVersionPair, topic: Topic, key: String, contentHash: String, message: String): Future[EventVersionPair] = {
    try {
      val currTxn = transactions.get(topic)
      require(currTxn == version.txnNo, s"Invalid transaction number: ${version.txnNo}, $currTxn")
      checkSeqNo(version, topic)
      
      doInsert(version, topic, key, contentHash, currTxn, message, isTxnBoundary = false)
    } catch {
      case NonFatal(e) =>
        Future.failed(e)
    }
  }

  private def doInsert(version: EventVersionPair, topic: Topic, key: String, contentHash: String, txnNo: Long,
                       message: String, isTxnBoundary: Boolean): Future[EventVersionPair] = {
    try {
      val initialNo = 0l

      val insert = (for {
        now <- curtime.result
        _ <- events +=(topic.name, version.seqNo, key, contentHash, message, now, version.txnNo, isTxnBoundary)
      } yield version).withTransactionIsolation(Serializable)

      val insertFuture = db.run(insert)

      insertFuture.map {
        case versionPair =>
          versions.put(topic, versionPair)
          topicInfoActor.foreach(_ ! TopicOffsetInfo(topic, versionPair))
          versionPair
      }
    } catch {
      case NonFatal(e) =>
        Future.failed(e)
    }
  }
  
  override def close(): Unit = {
    db.close()
  }
}

case class H2ESWriter(dbUrl: String, user: String = null, password: String = null, 
                      topicInfoActor: Option[ActorRef] = None)
                     (implicit val system: ActorSystem)
  extends DBWriter with slick.driver.H2Driver {
  
  Class.forName("org.h2.Driver")

  import api._

  override val db = Database.forURL(dbUrl, user, password)
}

case class SqlServerESWriter(dbUrl: String, user: String = null, password: String = null,
                             topicInfoActor: Option[ActorRef] = None)
                            (implicit val system: ActorSystem)
  extends DBWriter with freeslick.MSSQLServerProfile {

  import api._

  override val db = Database.forURL(dbUrl, user, password)
}