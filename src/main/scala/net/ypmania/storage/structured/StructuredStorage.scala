package net.ypmania.storage.structured

import com.twitter.util.ImmutableLRU

import net.ypmania.io.IO
import net.ypmania.storage.paged.PageIdx
import net.ypmania.storage.paged.PagedStorage

import StructuredStorage._
import akka.actor.Actor
import akka.actor.ActorLogging

import akka.actor.ActorRef
import akka.util.ByteString

class StructuredStorage(pagedStore: ActorRef) extends Actor with ActorLogging {
  var cache = ImmutableLRU[PageIdx, AnyRef](50)
  
  def receive = {
    case Write(pages, ctx) =>
      log.debug(s"Writing ${pages}")
      pages.foreach { case (pageIdx, (pageType, value)) =>
        cache = (cache + (pageIdx -> value))._2
      }
      val pageBytes = pages.mapValues { 
        case (pageType, value) => pageType.toByteString(value) }
      pagedStore ! PagedStorage.Write(pageBytes, Writing(sender, ctx))
    
    case PagedStorage.WriteCompleted(Writing(client, ctx)) =>
      client ! WriteCompleted(ctx)
    
    case read @ Read(page, ctx) =>
      val inCache = cache.get(page)
      cache = inCache._2
      if (inCache._1.isDefined) {
        log.debug(s"Found page ${page} in cache")
        sender ! ReadCompleted(inCache._1.get, ctx)
      } else {
        log.debug(s"Loading page ${page} from disk")
        pagedStore ! PagedStorage.Read(page, Reading(sender, page, read.pageType, ctx))
      }
      
    case PagedStorage.ReadCompleted(content, Reading(client, pageIdx, pageType, ctx)) =>
      log.debug(s"Parsing page ${pageIdx}")
      val value = pageType.fromByteString(content)
      cache = (cache + (pageIdx -> value))._2
      client ! ReadCompleted(value, ctx)
      
    case c @ Create(content, ctx) =>
      val bytes = c.pageType.toByteString(content)
      pagedStore ! PagedStorage.Create(bytes, Creating(sender, ctx))
      
    case PagedStorage.CreateCompleted(page, Creating(client, ctx)) =>
      client ! CreateCompleted(page, ctx)
      
    case other =>
      log.error(s"Received unexpected ${other}")
  }
}

object StructuredStorage {
  trait PageType[T <: AnyRef] {
    protected implicit val byteOrder = IO.byteOrder
    
    def fromByteString(page: ByteString): T
    def toByteString(page: T): ByteString
  }
  
  case class Read[T <: AnyRef] (page: PageIdx, ctx: AnyRef = None)(implicit val pageType: PageType[T])
  case class ReadCompleted(content: AnyRef, ctx: AnyRef)
  
  case class Write private (pages: Map[PageIdx, (PageType[AnyRef], AnyRef)], ctx: AnyRef) {
    def +[T <: AnyRef] (entry: (PageIdx, T))(implicit pageType: PageType[T]) =
      copy (pages = pages + ((entry._1, ((pageType.asInstanceOf[PageType[AnyRef]], entry._2)))))    
  }
  object Write {
    def apply():Write = new Write(Map.empty, None)
    def apply[T <: AnyRef](entry: (PageIdx, T))(implicit pageType: PageType[T]):Write = apply() + entry
  }
  case class WriteCompleted(ctx: AnyRef)
  
  case class Create[T <: AnyRef] (content: T, ctx: AnyRef = None)(implicit val pageType: PageType[T])
  case class CreateCompleted(page: PageIdx, ctx: AnyRef)
  
  private case class Reading(sender: ActorRef, pageIdx: PageIdx, pageType: PageType[AnyRef], ctx: AnyRef)
  private case class Writing(sender: ActorRef, ctx: AnyRef)
  private case class Creating(sender: ActorRef, ctx: AnyRef)
}