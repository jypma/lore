package net.ypmania.lore.event

import net.ypmania.storage.paged.PagedStorage
import net.ypmania.lore.ID
import akka.util.ByteString
import net.ypmania.io.IO._

case class EventsPage(events: Map[ID,Event]) {
  def + (event: Event) = EventsPage(events + (event.id -> event))
}

object EventsPage {
  val empty = EventsPage(Map.empty)
  
  implicit object Type extends PagedStorage.PageType[EventsPage] {
    
    def fromByteString(bytes: ByteString) = {
      val i = bytes.iterator
      val count = i.getInt
      val events = Map.newBuilder[ID,Event]
      for (idx <- 0 until count) {
        val event = Event(i)
        events += event.id -> event
      }
      EventsPage(events.result)
    }
    
    def toByteString(page: EventsPage) = {
      val bs = ByteString.newBuilder
      bs.putInt(page.events.size)
      for (event <- page.events.values) {
        event.write(bs)
      }
      bs.result
    }
    
    def empty = EventsPage.empty    
  }
  
}