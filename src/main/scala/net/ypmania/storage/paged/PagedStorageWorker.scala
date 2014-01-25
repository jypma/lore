package net.ypmania.storage.paged

import net.ypmania.io.FileActor
import net.ypmania.io.IO._

import PagedStorage._

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.PoisonPill

trait PagedStorageWorker extends Actor with ActorLogging {
  import PagedStorageWorker._
  
  def receive = { // move this into AbstractActor?
    case _ => throw new IllegalStateException ("Not ready yet")
  }
  
  private case class WriteQueueEntry (sender: ActorRef, write: Write) {
    private var inProgress = write.pages.size
    def done = { inProgress <= 0 }
    def finishPage(page: PageIdx) {
      if (done) throw new Exception("Decremented more writes than we sent out...")
      inProgress -= 1;  
    }
  }
  private case class Reading[T <: AnyRef](sender: ActorRef, read: Read[T])
  private case class Writing(entry: WriteQueueEntry, page: PageIdx)
  
  def work(dataFile: ActorRef, journalFile: ActorRef, dataHeader: DataHeader, journalHeader: JournalHeader, 
              initialJournalIndex: Map[PageIdx, Long], initialPages: PageIdx, initialJournalPos: Long): Unit = {
    case object SyncForShutdown
    
    var journalIndex = collection.mutable.Map.empty[PageIdx,Long] ++ initialJournalIndex
    var journalPos = initialJournalPos
    var pageCount = initialPages
    var writeQueue = Vector.empty[WriteQueueEntry]
    var writing = collection.mutable.Map.empty[PageIdx, WriteQueueEntry]
  
    def performWrite(q: WriteQueueEntry) {
      val journalEntry = JournalEntry(journalHeader, q.write.pageBytes)
      val content = journalEntry.toByteString
      journalFile ! FileActor.Write(journalPos, content, q)
      
      journalPos += SizeOf.MD5
      journalPos += SizeOf.Int // number of pages
      for (page <- q.write.pages.keys) {
        if (page >= pageCount) {
          pageCount = page + 1
        }
        writing(page) = q
        
        journalPos += SizeOf.PageIdx
        log.debug(s"Stored page ${page} at ${journalPos}")
        journalIndex(page) = journalPos
        journalPos += journalHeader.pageSize
      }
    }
    
    def emptyWriteQueue() {
      writeQueue.foreach(performWrite)
      writeQueue = Vector.empty
    }
    
    def sync() {
      journalFile ! FileActor.Sync(SyncForShutdown)
    }
    
    context.become {
      case read:Read[_] =>
        log.debug(s"processing read for ${sender}")
        if (read.page >= pageCount) 
          throw new Exception (s"Trying to read page ${read.page} but only have ${pageCount}")
        writing.get(read.page).map { entry =>
          log.debug(s"Replying in-transit write content to ${sender}")
          val contentBeingWritten = entry.write.pages(read.page)
          sender ! ReadCompleted(contentBeingWritten._2, read.ctx)
        }.getOrElse {
          journalIndex.get(read.page).map { pos =>
            log.debug(s"Found page ${read.page} in journal at pos ${pos}")
            journalFile ! FileActor.Read(pos, journalHeader.pageSize, Reading(sender, read))
          }.getOrElse {
            val pos = dataHeader.offsetForPage(read.page)
            log.debug(s"Reading page ${read.page} from data at pos ${pos} for ${sender}")
            dataFile ! FileActor.Read(pos, dataHeader.pageSize, Reading(sender, read))          
          }  
        }
        
      case FileActor.ReadCompleted(content, Reading(replyTo, read)) =>  
       replyTo ! ReadCompleted(read.pageType.fromByteString(content), read.ctx)
        
      case write:Write =>
        //TODO also remove this page from freelist
        write.pageBytes.foreach { case (page, content) =>
          if (content.length > journalHeader.pageSize) 
      	  throw new Exception(s"Content length ${content.length} for page ${page} overflows page size ${journalHeader.pageSize}")
        }
        val q = WriteQueueEntry(sender, write)
        if (writing.isEmpty) {
          performWrite(q)
        } else {
          writeQueue :+= q
        }
        
      case FileActor.WriteCompleted(entry:WriteQueueEntry) =>
        entry.sender ! WriteCompleted(entry.write.ctx)
        entry.write.pages.keys.foreach(writing.remove)
        if (!writing.isEmpty) {
          log.warning("Writing log was not empty after completing a write. Concurrency bug.")
        }
        emptyWriteQueue()
        
      case Shutdown =>
        //log.debug("Shutdown request, syncing journal")
        emptyWriteQueue()
        sync()
        
      case FileActor.SyncCompleted(SyncForShutdown) =>
        //log.debug("Journal synced, poisining ourselves")
        self ! PoisonPill
        
      case create: Create[_] =>
        import create.pageType
        //TODO also update freelist with this page
        self ! Write(pageCount -> create.content, Creating(sender, CreateCompleted(pageCount, create.ctx)))
        pageCount += 1

      case WriteCompleted(Creating(client, response)) =>
        client ! response
        
      case other =>
        log.error("Dropping {}", other)
      }
  
  }
}

object PagedStorageWorker {
  private case class Creating (sender: ActorRef, response: CreateCompleted)
}