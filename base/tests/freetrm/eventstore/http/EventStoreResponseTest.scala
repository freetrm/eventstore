package freetrm.eventstore.http

import freetrm.eventstore._
import org.scalatest.{FunSuite, Matchers}
import spray.json._
import fommil.sjs.FamilyFormats._

class EventStoreResponseTest extends FunSuite with Matchers {
  private def jsonRep(resp: EventStoreResponse) = resp.toJson

  private def assertRoundTrip(resp: EventStoreResponse) = {
    resp.toJson.prettyPrint.parseJson.convertTo[EventStoreResponse] shouldEqual resp
  }

  test("serialisation and deserialisation") {
    val result = EventStoreResults(Vector(
      EventTransactionStart(EventVersionPair(0l, 0l)),
      EventSourceEvent(EventVersionPair(10l, 2l), "key1", "cv1", "data1"),
      EventSourceEvent(EventVersionPair(3l, 7l), "key2", "cv2", "data2"),
      EventTransactionEnd(EventVersionPair(4l, 9l))
    ))

    jsonRep(result) shouldEqual
      """
        {
          "type": "EventStoreResults",
          "results": [{
            "type": "EventTransactionStart",
            "version": {
              "seqNo": 0,
              "txnNo": 0
            }
          }, {
            "data": "data1",
            "key": "key1",
            "version": {
              "seqNo": 10,
              "txnNo": 2
            },
            "type": "EventSourceEvent",
            "contentHash": "cv1"
          }, {
            "data": "data2",
            "key": "key2",
            "version": {
              "seqNo": 3,
              "txnNo": 7
            },
            "type": "EventSourceEvent",
            "contentHash": "cv2"
          }, {
            "type": "EventTransactionEnd",
            "version": {
              "seqNo": 4,
              "txnNo": 9
            }
          }]
        }
      """.parseJson
    assertRoundTrip(result)
  }

}
