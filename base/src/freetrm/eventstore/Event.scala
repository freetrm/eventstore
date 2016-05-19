package freetrm.eventstore


case class EventVersionPair(seqNo: Long, txnNo: Long) {
  def incrementSeq = copy(seqNo = seqNo + 1)

  def incrementTxn = copy(txnNo = txnNo + 1)
}

case class Topic(name: String)

sealed trait Event {
  def version: EventVersionPair
}

case class EventTransactionStart(version: EventVersionPair) extends Event

case class EventTransactionEnd(version: EventVersionPair) extends Event

case class EventInvalidate(version: EventVersionPair) extends Event

case class EventSourceEvent(version: EventVersionPair, key: String, contentHash: String, data: String)
  extends Event



sealed trait EventStoreResponse

case class EventStoreResponseOK(version: EventVersionPair) extends EventStoreResponse

case class EventStoreResponseDuplicate(duplicate: EventVersionPair) extends EventStoreResponse

case class EventStoreResponseError(message: String, exception: String) extends EventStoreResponse

case class EventStoreResults(results: Vector[Event]) extends EventStoreResponse


case class EventNotification(offsets: Map[Topic, EventVersionPair])
