package net.ypmania.storage.paged

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.util.ByteString
import akka.actor.Stash
import net.ypmania.io.IO

class PagedStorage(filename: String) extends Actor with Stash with ActorLogging with PagedStorageWorker with PagedStorageOpener {
  open(filename)
}

object PagedStorage {
  trait PageType[T <: AnyRef] {
    protected implicit val byteOrder = IO.byteOrder
    
    def fromByteString(page: ByteString): T
    def toByteString(page: T): ByteString
  }
  
  case class Read[T <: AnyRef] (page: PageIdx, ctx: AnyRef = None)(implicit val pageType: PageType[T])
  case class ReadCompleted[T <: AnyRef] (content: T, ctx: AnyRef) 
  
  case class Write private (pages: Map[PageIdx, (PageType[AnyRef], AnyRef)], ctx: AnyRef) {
    def +[T <: AnyRef] (entry: (PageIdx, T))(implicit pageType: PageType[T]) =
      copy (pages = pages + ((entry._1, ((pageType.asInstanceOf[PageType[AnyRef]], entry._2)))))    
    lazy val pageBytes = pages.mapValues { case (pageType, value) => pageType.toByteString(value) }
  }
  object Write {
    def apply[T <: AnyRef](entry: (PageIdx, T), ctx: AnyRef = None)(implicit pageType: PageType[T]):Write = 
      new Write(Map.empty, ctx) + entry
  }
  case class WriteCompleted(ctx: AnyRef)

  case class Create[T <: AnyRef](content: T, ctx: AnyRef = None)(implicit val pageType: PageType[T]) 
  case class CreateCompleted(page: PageIdx, ctx: AnyRef)
  
  case object Shutdown
}