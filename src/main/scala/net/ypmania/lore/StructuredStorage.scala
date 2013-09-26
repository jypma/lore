package net.ypmania.lore

import akka.actor.ActorRef
import akka.actor.ActorLogging
import akka.actor.Actor
import akka.util.ByteString

class StructuredStorage(pagedStore: ActorRef) extends Actor with ActorLogging {
  val cache = collection.mutable.Map.empty[PageIdx, AnyRef]
  
  def receive = {
    //case Read
    
    
    case other =>
      log.error(s"Received unexpected ${other}")
  }
}

object StructuredStorage {
  trait PageType[T] {
    implicit val byteOrder = PagedStorage.byteOrder
    
    def fromByteString(page: ByteString): T
    def toByteString(page: T): ByteString
  }
  
  case class Read[T] (page: PageIdx, pageType: PageType[T], ctx: AnyRef = None)
}