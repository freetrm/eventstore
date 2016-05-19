package freetrm.eventstore.http

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.{ActorMaterializer, KillSwitches}
import akka.testkit.TestKit
import freetrm.eventstore._
import freetrm.eventstore.http.HttpWriter.{WriteEvent, WriteEventData, WriteTransactionEnd, WriteTransactionStart}
import org.scalamock.scalatest.MockFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSuiteLike, Matchers}

import scala.concurrent.Future

class HttpReaderAndWriterTest extends TestKit(ActorSystem()) with FunSuiteLike with FunSuiteDoc
  with ScalaFutures with Matchers with MockFactory with BeforeAndAfterAll with ESTestUtils {

  implicit val mat = ActorMaterializer()
  implicit val defaultPatience = PatienceConfig(timeout = Span(15, Seconds), interval = Span(100, Millis))
  val topic = Topic("topic")
  
  documentTests(
    "topic exists" testsThat 
      """
         If you create a topic then call HttpReader it will confirm it exists. Also tests the case
         where a topic doesn't exist.
      """,

    "http publish and consume" testsThat
      """"
        If you create events, publish them using HttpWriter and then consume them using HttpReader you get
        the same events back.
        This is a full end to end test. It starts a real http server rather than mocking that bit out.
      """,
    
    "writer exception" testsThat
      """"
        If the underlying EventStoreWriter throws (in this case because of invalid input) then the exception
        propagates back to the HttpWriter caller and the Source terminates.
      """,
    
    "exception while calling consume" testsThat
      """"
        When the HttpServer is stopped, while a consumer is active, the close exception is propagated to the 
        consumer and the consumers Source terminates.
      """,
    
    "event surface" testsThat
      """"
        A call to /es/surface will return just the latest event for each key in that topic.
      """,
    
    "duplicates" testsThat
      """"
        Publishing a duplicate Event responds saying it was a duplicate and that further publishing is still fine
        even though there is now a gap in the sequence numbers. 
      """
  )

  test("topic exists") {
    implicit val mat = ActorMaterializer()
    val port = TestUtils.choosePort

    val eventSourceReader = mock[EventSourceReader]

    
    val topic2 = Topic("topic2")
    (eventSourceReader.topicExists _).expects(topic2).returns(Future.successful(false)).once()
    (eventSourceReader.topicExists _).expects(topic).returns(Future.successful(true)).once()

    val ws = new EventStoreHttpServer(mock[EventSourceWriter], eventSourceReader, "cookie")
    Http().bindAndHandle(ws.flow, "localhost", port).futureValue
    val httpReader = new HttpReader("localhost", port, "username", "cookie")

    httpReader.topicExists(topic2).futureValue shouldBe false
    httpReader.topicExists(topic).futureValue shouldBe true
  }

  test("http publish and consume") {
    implicit val mat = ActorMaterializer()
    val port = TestUtils.choosePort

    val (writer, events) = inMemESWriter(topic)
    val reader = esReader(topic, events)

    val ws = new EventStoreHttpServer(writer, reader, "cookie")
    Http().bindAndHandle(ws.flow, "localhost", port).futureValue
    val httpReader = new HttpReader("localhost", port, "username", "cookie")
    val httpWriter = new HttpWriter("localhost", port, "username", "cookie")

    httpReader.topicExists(topic).futureValue shouldBe false

    val source = createEvents(topic, 100)

    val resultsOfPublish = httpWriter.publish(topic, source).runWith(Sink.seq).futureValue

    var txn = 0
    val expectedWriteResults = (0 until 100).map {
      seqNo =>
        val resp = EventStoreResponseOK(EventVersionPair(seqNo, txn))
        if ((seqNo + 1) % 10 == 0) {
          txn += 1
        }
        resp
    }
    resultsOfPublish shouldEqual expectedWriteResults


    txn = 0
    val expectedReadResults = (0 until 100).map {
      seqNo =>
        if (seqNo % 10 == 0) {
          EventTransactionStart(EventVersionPair(seqNo, txn))
        } else if ((seqNo + 1) % 10 == 0) {
          txn += 1
          EventTransactionEnd(EventVersionPair(seqNo, txn - 1))
        } else {
          EventSourceEvent(EventVersionPair(seqNo, txn), s"key$seqNo" , "hash" + seqNo, "data" + seqNo)
        }
    }

    val maxSeqNo = httpReader.maxSeqNo(topic).futureValue
    //    httpReader.consume(topic, 0, None).take(100).runWith(Sink.seq).futureValue shouldEqual expectedReadResults
    val consume = httpReader.streamEvents(topic, 0, maxSeqNo).runWith(Sink.seq).futureValue
    consume shouldEqual expectedReadResults
  }

  test("writer exception") {
    val (httpReader, httpWriter, _, _) = createHttpReaderAndWriter(topic)

    var i = -1
    val source = createEvents(topic, 1000)
    val brokenSource: Source[Event, NotUsed] = source.map {
      event =>
        i += 1
        // when we get to 85 we'll start a new transaction which will cause an exception
        // as we're already in a transaction
        if (event.version.seqNo == 85) {
          EventTransactionStart(event.version)
        } else {
          event
        }
    }

    intercept[Exception] {
      httpWriter.publish(topic, brokenSource).runForeach(identity).futureValue
    }.getMessage should include("Already in a transaction")

    // we killed the stream at 85. make sure no subsequent events went through.
    httpReader.maxSeqNo(topic).futureValue shouldEqual Some(84)
    
    // start off again from where we left off
    val newSource = source.dropWhile(_.version.seqNo <= 84)
    httpWriter.publish(topic, newSource).runForeach(identity).futureValue

    httpReader.maxSeqNo(topic).futureValue shouldEqual Some(999)
  }

  test("exception while calling consume") {
    val (httpReader, httpWriter, binding, serverActorSystem) = createHttpReaderAndWriter(topic)

    val source = createEvents(topic, 10000000)

    httpWriter.publish(topic, source).runForeach(identity)
    httpReader.streamEvents(topic, 0, None).take(100).runWith(Sink.seq).futureValue.size shouldEqual 100

    intercept[Exception] {
      val source = httpReader.streamEvents(topic, 0, None)
      
      source.runForeach {
        _ =>
          // kill the http server
          binding.unbind().futureValue
          // binding.unbind() doesn't do anything. we need to kill the actor system used by the http server
          serverActorSystem.terminate()
      }.futureValue
    }.getMessage should include("The connection closed with error: Connection reset by peer")

  }
  
  test("event surface") {
    val (httpReader, httpWriter, binding, serverActorSystem) = createHttpReaderAndWriter(topic)

    // so at the end we should have data events 449 to 498 (499 is the 'end trasaction' event)
    val source = createEvents(topic, 500, key = i => s"key${i % 50}", txnEveryN = 500)

    httpWriter.publish(topic, source).runForeach(identity).futureValue
    val surface = httpReader.latestSurface(topic).runWith(Sink.seq).futureValue
    surface.size shouldEqual 50
    surface shouldEqual (449 until 499).map {
      i =>
        EventSourceEvent(EventVersionPair(i, 0), s"key${i % 50}", s"hash$i", "data" + i)
    }
  }
  
  test("duplicates") {
    val (httpReader, httpWriter, binding, serverActorSystem) = createHttpReaderAndWriter(topic)

    val source = createEvents(topic, 500, key = i => s"key${i % 50}", hash = i => "", txnEveryN = 25)

    httpWriter.publish(topic, source).runForeach(identity).futureValue
    
    val events = httpReader.streamEvents(topic, 0, httpReader.maxSeqNo(topic).futureValue).runWith(Sink.seq).futureValue
    
    // we have a txn every 25 events. so that's 23 data events between each one. we do % 50 above on the key 
    // and use the same hash for each event. so that means we will have 23 * 2 unique events. all the rest
    // will be thrown away as duplicates.
    events.count(_.isInstanceOf[EventSourceEvent]) shouldEqual 23 * 2
  }


  private def createHttpReaderAndWriter(topic: Topic, readerOverride: Option[EventSourceReader] = None) = {
    val port = TestUtils.choosePort
    val (writer, events) = inMemESWriter(topic)
    val reader = readerOverride.getOrElse(esReader(topic, events))

    val ws = new EventStoreHttpServer(writer, reader, "cookie")
    val ks = KillSwitches.shared("fortesting")

    val serverActorSystem = ActorSystem()
    val binding = Http()(serverActorSystem).bindAndHandle(ws.flow.via(ks.flow), "localhost", port).futureValue
    val httpReader = new HttpReader("localhost", port, "username", "cookie")
    val httpWriter = new HttpWriter("localhost", port, "username", "cookie")
    (httpReader, httpWriter, binding, serverActorSystem)
  }

  // Creates HttpEvents where every eight events are in a transaction
  private def createEvents(topic: Topic, 
                           howMany: Int,
                           fromVersion: Option[EventVersionPair] = None,
                           key: Int => String = i => s"key$i",
                           hash: Int => String = i => s"hash$i",
                           txnEveryN: Int = 10): Source[Event, NotUsed] = {
    Source.fromIterator(() => (0 until howMany).toIterator.map {
      seqNo =>
        if (seqNo % txnEveryN == 0) {
          WriteTransactionStart
        } else if ((seqNo + 1) % txnEveryN == 0) {
          WriteTransactionEnd
        } else {
          WriteEventData(s"${key(seqNo)}", hash(seqNo), "data" + seqNo)
        }
    }).via(convertEvents(fromVersion))
  }

  private def convertEvents(version: Option[EventVersionPair]): Flow[WriteEvent, Event, NotUsed] = {
    Flow[WriteEvent].statefulMapConcat(() => {
      var currentVersion = version.getOrElse(EventVersionPair(seqNo = -1, txnNo = -1))
      x => x match {
        case WriteTransactionStart =>
          val newVersion = currentVersion.incrementSeq.incrementTxn
          currentVersion = newVersion
          EventTransactionStart(newVersion) :: Nil
        case WriteTransactionEnd =>
          val newVersion = currentVersion.incrementSeq
          currentVersion = newVersion
          EventTransactionEnd(newVersion) :: Nil
        case WriteEventData(key, hash, data) =>
          val newVersion = currentVersion.incrementSeq
          currentVersion = newVersion
          EventSourceEvent(newVersion, key, hash, data) :: Nil
      }
    })
  }
}
