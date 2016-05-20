package org.freetrm.eventstore.db

import java.sql.Timestamp

import slick.driver.{JdbcProfile, JdbcDriver}
import slick.profile.RelationalProfile.ColumnOption.Length
import slick.profile.SqlProfile.ColumnOption.SqlType

import spray.json._

class Tables(val driver: JdbcProfile) {
  import driver.api._


  case class EventsSchema(tag: Tag) 
    extends Table[(String, Long, String, String, String, Timestamp, Long, Boolean)](tag, Tables.EventsTable) {
    
    def topic = column[String]("TOPIC", Length(40, varying = true))

    def sequenceNumber = column[Long]("SEQUENCE_NUMBER")

    def eventID = column[String]("EVENT_ID", Length(40, varying = true))
    
    def contentHash = column[String]("CONTENT_HASH", Length(255, varying = true))
    
    def data = column[String]("DATA", SqlType("TEXT"))

    def timestamp = column[Timestamp]("TIMESTAMP")

    def txnId = column[Long]("TXN_NUMBER")
    
    def isMetadata = column[Boolean]("IS_METADATA")

    def pk = primaryKey("pk_events", (topic, sequenceNumber))

    def * = (topic, sequenceNumber, eventID, contentHash, data, timestamp, txnId, isMetadata)
  }

}

object Tables {
  
  val EventsTable = "EVENTS"
  
  val TxnStartData = "Transaction Start"
  val TxnEndData = "Transaction End"
}
