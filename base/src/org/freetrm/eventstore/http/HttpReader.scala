package org.freetrm.eventstore.http

import java.net.URLEncoder

import akka.NotUsed
import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.client.RequestBuilding._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Accept, Authorization, BasicHttpCredentials}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import de.heikoseeberger.akkasse.MediaTypes.`text/event-stream`
import de.heikoseeberger.akkasse.{EventStreamUnmarshalling, ServerSentEvent}
import fommil.sjs.FamilyFormats._
import org.freetrm.eventstore._
import org.freetrm.eventstore.utils._
import spray.json.JsonParser.ParsingException
import spray.json._

import scala.concurrent.Future
import scala.util.control.NonFatal

class HttpReader(host: String, port: Int, username: String, cookie: String)
                (implicit system: ActorSystem)
  extends EventSourceReader with Log {

  import EventStreamUnmarshalling._

  implicit val mat = ActorMaterializer()
  implicit val dispatcher = system.dispatcher

  private val credentials = Authorization(BasicHttpCredentials(username, cookie))
  private val streamHeader = Accept(`text/event-stream`)
  private val textHeader = Accept(MediaTypes.`text/plain`)
  private val headers = List(credentials, streamHeader, textHeader)

  override def streamEvents(topic: Topic,
                            earliestOffsetToReadFrom: Long,
                            lastOffsetToReadTo: Option[Long]): Source[Event, NotUsed] = {


    val uri = s"/es/consume/${enc(topic)}/$earliestOffsetToReadFrom/" + lastOffsetToReadTo.map(_.toString).getOrElse("")
    val req: Source[HttpResponse, NotUsed] = getRequest(uri)

    consume[Event](req, response => s"Failed calling 'consume' for $topic: $response")
  }
  
  override def streamNotifications: Source[EventNotification, NotUsed] = {
    val uri = "/es/consumeNotifications"
    val req: Source[HttpResponse, NotUsed] = getRequest(uri)

    consume[EventNotification](req, response => s"Failed calling 'consumeNotifications': $response")
  }

  override def latestSurface(topic: Topic): Source[EventSourceEvent, NotUsed] = {
    val uri = s"/es/surface/${enc(topic)}"
    val req: Source[HttpResponse, NotUsed] = getRequest(uri)

    consume[EventSourceEvent](req, response => s"Failed calling 'surface': $response")
  }

  private def consume[T](source: Source[HttpResponse, NotUsed], failMsg: String => String)
                        (implicit writer: spray.json.JsonReader[T]) = {
    source
      .mapAsync(1) { // flatMap the future the Unmarshall below will produce. Just happens once so '1' here is fine.
        response =>
          if (response.status == StatusCodes.OK) {
            Unmarshal(response).to[Source[ServerSentEvent, Any]]
          } else {
            val errorMessage = response.entity match {
              case e: HttpEntity.Strict =>
                e.data.decodeString("UTF-8")
              case _ =>
                "No error message"
            }
            val msg = failMsg(errorMessage)
            log.error(msg)
            throw new Exception(msg)
          }
      }
      .flatMapConcat(
        _.filter {
          _ != ServerSentEvent.heartbeat
        }.map {
          event => try {
            event.data.parseJson.convertTo[T]
          } catch {
            case NonFatal(e) =>
              log.error("Failed to parse: " + event.data)
              throw e
          }
        })
  }

  override def close(): Unit = close(None)

  override def topicExists(topic: Topic): Future[Boolean] = {
    jsonRPC[Boolean](s"/es/topicExists/${enc(topic)}")
  }

  override def listTopics: Future[Seq[Topic]] = {
    jsonRPC[Seq[String]](s"/es/listTopics").map(topics => topics.map(Topic))
  }

  override def maxTxnNo(topic: Topic): Future[Option[Long]] = {
    jsonRPC[Option[Long]](s"/es/maxTxnNo/${enc(topic)}")
  }

  override def maxSeqNo(topic: Topic): Future[Option[Long]] = {
    jsonRPC[Option[Long]](s"/es/maxSeqNo/${enc(topic)}")
  }

  def close(t: Option[Throwable]) = {
    log.info("HttpReader.close called")
  }

  private def getRequest(uri: String) = {
    require(uri.head == '/', s"Invalid URI (needs leading slash): $uri")
    Source.single(Get(Uri(uri)).withHeaders(credentials))
      .via(Http().outgoingConnection(host, port))
  }

  private def jsonRPC[T](uri: String)(implicit evidence: spray.json.JsonReader[T]) = {
    val req = getRequest(uri)
    val result = req
      .mapAsync(1)(response => Unmarshal(response.entity).to[String]) // flatMap the future Unmarshal produces
      .map { json =>
        try {
          json.parseJson.convertTo[T]
        } catch {
          case e: ParsingException =>
            val msg = s"Failed to call $uri, result: $json"
            log.error(msg)
            throw new Exception(msg)
        }
      }
    result.runWith(Sink.head)
  }

  private def enc(s: String) = URLEncoder.encode(s, "UTF-8")
  private def enc(t: Topic) = URLEncoder.encode(t.name, "UTF-8")
}
