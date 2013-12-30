package net.ypmania.storage.paged

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.util.ByteString

class PagedStorage(filename: String) extends Actor with ActorLogging with PagedStorageWorker with PagedStorageOpener {
  open(filename)
}

object PagedStorage {
  case class Read(page: PageIdx, ctx: AnyRef = None)
  case class ReadCompleted(content: ByteString, ctx: AnyRef)
  
  case class Write(pages: Map[PageIdx, ByteString], ctx: AnyRef = None)
  object Write {
    def apply(page: PageIdx, content: ByteString, ctx:AnyRef) = new Write(Map(page -> content), ctx)
    def apply(page: PageIdx, content: ByteString) = new Write(Map(page -> content), None)
  }
  case class WriteCompleted(ctx: AnyRef)

  case class Create(content: ByteString, ctx: AnyRef = None)
  case class CreateCompleted(page: PageIdx, ctx: AnyRef)
  
  case object Ready
  
  case object Shutdown
}