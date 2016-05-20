package org.freetrm.eventstore.http

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.Config
import org.freetrm.eventstore.utils.Log
import org.freetrm.eventstore.{EventSourceReader, EventSourceWriter}
import scaldi.Module

import scala.concurrent.Future
import scala.concurrent.duration._


case class Request(client: ActorRef, req: HttpRequest)

class WebService extends Module with Log {
  implicit lazy val system = inject[ActorSystem]
  implicit lazy val mat = ActorMaterializer()

  def start(): Future[ServerBinding] = {

    val conf = inject[Config]

    implicit val timeout = Timeout(5.seconds)
    val interface = conf.getString("www-service.interface")
    val port = conf.getInt("www-service.port")

    log.info(s"Starting http server on $interface:$port")
    
    Http().bindAndHandle(service.flow, interface, port)
  }

  def stop() {}

  def service: EventStoreHttpServer = {
    implicit val system = inject[ActorSystem]
    val conf = inject[Config]

    val cookie = conf.getString("www-service.cookie")
    new EventStoreHttpServer(
      inject[EventSourceWriter],
      inject[EventSourceReader], 
      cookie)
  }
}
