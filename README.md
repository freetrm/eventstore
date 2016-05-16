
# EventStore

### What is it?

This is an event sourcing abstraction that allows you to swap out the server event store implementation. The code currently supports using a database as the event store. To add difference back-ends you would have to implement [EventSourceWriter](base/src/freetrm/eventstore/EventSourceWriter.scala) and [EventSourceReader](base/src/freetrm/eventstore/EventSourceReader.scala).

Events are written and read using an HTTP based API.

### Why not use [Apache Kafka] (or something similar)

We had several feature requirements that Apache Kafka doesn't have.

 * We needed de-duplication of events to happen. So when you post an event to the eventstore it will check if there is a previously written event in the same topic with the same key and if there is whether the hash for that event is the same as this one. If that's the case it throws away the new `Event` and returns the `Version` of the previous `Event`.
 
 * We needed a concept of a transaction that grouped events together. So if several events should be logically seen together and it's not valid to take them separately then they are in the same transaction. A transaction is an incrementing long that does not cross topics.
  
 * Most companies will already have a database infrastructure that is managed. Not a lot of places will already have a managed [Apache Kafka] and [Apache Zookeeper] set up and may not be willing to invest in one. The database back-end is several orders of magnitude slower than [Apache Kafka] but that may not be an issue for many people. If it is then code is written so that we could swap in [Apache Kafka] as the back-end.
   
 * We needed to be able to show the 'surface' of a topic. The surface is just all the newest events for each key for a topic. This needed for diffing (using something like [LShift Diffa]) with an non-versioned upstream system. By non-versioned I mean that updates to entities overwrite older versions.
 
 * We needed a way to say that a previous transaction was invalid without having to delete the event(s). For example if someone produced an invalid message that couldn't be processed. Downstream would see the invalid `Event` but continue processing and record that there is a problem. Once it saw a `EventInvalidate` `Event` for that transaction it would be able to clear the invalid state. 


### What back-ends are supported?

Just a database back-end for the moment. The DB layer uses [Slick] (and [FreeSlick] for the drivers Slick [closed](http://slick.lightbend.com/doc/2.0.0/extensions.html)) with implementations for H2 and MS SqlServer. Other implementations are easy to add (a few lines of code).

### How does it work?

Have a look in this [Main](db/src/freetrm/eventstore/db/Main.scala) class for an example of creating the underlying `DBEventSourceReader` and the DB `EventSourceWriter`. Once you have a reader and a writer you can create a `EventStoreHttpServer` which exposes the reader and writer through HTTP POSTs (for writes) and GETs (for reads).

Once you have the `EventStoreHttpServer` running listening on a port you can create a `HttpReader` and `HttpWriter` and point them at that host and port.

### Releases

None yet.

### Any problems?

* It's very slow to publish through the `EventStore`. You get about 200 writes per second. This is because we wait for each write to be acknowledged before writing the next one. If we didn't do this and instead wrote as fast as possible you could end up in a situation where an `Event` failed to write but a subsequent `Event` succeeded. This would leave you in an invalid state.

* The [Akka HTTP] POST code is a bit too magic. It doesn't work the way it should (exceptions don't propagate and kill the Flow correctly) so I've had to hack around that. I'm tempted to take it out and try something else. 


[//]: #
[Apache Kafka]: http://kafka.apache.org/
[Apache HTTP]: http://doc.akka.io/docs/akka/current/scala/http/
[Apache ZooKeeper]: http://zookeeper.apache.org/
[Slick]: http://slick.typesafe.com
[FreeSlick]: https://github.com/smootoo/freeslick
[LShift Diffa]: https://github.com/lshift/diffa
