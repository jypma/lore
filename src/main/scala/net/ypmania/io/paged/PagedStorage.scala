package net.ypmania.io.paged

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.util.ByteString

class PagedStorage(filename: String) extends Actor with ActorLogging with PagedStorageWorker with PagedStorageOpener {
  open(filename)
}

object PagedStorage {
//  def props (dataFile: ActorRef, journalFile: ActorRef, dataHeader: DataHeader, journalHeader: JournalHeader, 
//                initialJournalIndex: Map[PageIdx, Long], initialPages: PageIdx, initialJournalPos: Long) =
//    Props(classOf[PagedStorage], dataFile, journalFile, dataHeader, journalHeader, initialJournalIndex, initialPages.toInt, initialJournalPos)
  
  case class Read(page: PageIdx, ctx: AnyRef = None)
  case class ReadCompleted(content: ByteString, ctx: AnyRef)
  
  case class Write(pages: Map[PageIdx, ByteString], ctx: AnyRef = None)
  object Write {
    def apply(page: PageIdx, content: ByteString, ctx:AnyRef) = new Write(Map(page -> content), ctx)
    def apply(page: PageIdx, content: ByteString) = new Write(Map(page -> content), None)
  }
  case class WriteCompleted(ctx: AnyRef)

  case object Ready
  
  case object Shutdown
}