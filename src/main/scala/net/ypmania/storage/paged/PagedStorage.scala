package net.ypmania.storage.paged

import java.nio.file.Paths
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.WRITE
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.Stash
import akka.actor.SupervisorStrategy
import akka.actor.Terminated
import akka.pattern.pipe
import akka.pattern.ask
import akka.util.ByteString
import net.ypmania.io.FileActor
import net.ypmania.io.IO
import net.ypmania.io.IO.SizeOf
import akka.util.Timeout
import scala.concurrent.duration._

class PagedStorage(filename: String) extends Actor with Stash with ActorLogging {
  import PagedStorage._
  
  implicit val timeout = Timeout(1.minute)
  implicit val executionContext = context.dispatcher
  
  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy
  
  private val dataFile = dataFileActor()
  private val journalFile = journalFileActor()
  private val opener = openerActor()

  context.watch(dataFile)
  context.watch(journalFile)
  context.watch(opener)
  
  def dataFileActor() = context.actorOf(FileActor.props(Paths.get(filename), Seq(READ, WRITE, CREATE)), "d")
  def journalFileActor() = context.actorOf(FileActor.props(Paths.get(filename + ".j"), Seq(READ, WRITE, CREATE)), "j")
  def openerActor() = context.actorOf(Props(new PagedStorageOpener(self, dataFile, journalFile)), "o")
  
  def receive = { 
    case openedState:InitialState =>
      unstashAll()
      work(openedState)
      
    case Terminated(_) =>
      throw new IllegalStateException(s"Could not open ${filename}")
      
    case _ =>
      stash()
  } 
  
  private def readFrom(file: ActorRef, pos: Long, size: Int)(conv: ByteString => AnyRef) {
    import FileActor._
    file ? Read(pos, size) map { case ReadCompleted(bytes) => conv(bytes) } pipeTo self    
  }
  
  def work(initial: InitialState): Unit = {
      
    import initial.journalHeader
    import initial.dataHeader
    
    var journalIndex = collection.mutable.Map.empty[PageIdx,Long] ++ initial.journalIndex
    var journalPos = initial.journalPos
    var pageCount = initial.pageCount
    var writeQueue = Vector.empty[WriteQueueEntry]
    var writing = collection.mutable.Map.empty[PageIdx, WriteQueueEntry]
  
    def performWrite(q: WriteQueueEntry) {
      val journalEntry = JournalEntry(journalHeader, q.write.pageBytes)
      val content = journalEntry.toByteString
      journalFile ? FileActor.Write(journalPos, content) map {
        case FileActor.WriteCompleted => QueueEntryWritten(q)
      } pipeTo self
      
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
      journalFile ? FileActor.Sync map { _ => SyncForShutdown} pipeTo self
    }
    
    context.become {
      case read:Read[_] =>
        log.debug(s"processing read for ${sender}")
        if (read.page >= pageCount) 
          throw new Exception (s"Trying to read page ${read.page} but only have ${pageCount}")
        writing.get(read.page).map { entry =>
          log.debug(s"Replying in-transit write content to ${sender}")
          val contentBeingWritten = entry.write.pages(read.page)
          sender ! ReadCompleted(contentBeingWritten.content)
        }.getOrElse {
          journalIndex.get(read.page).map { pos =>
            log.debug(s"Found page ${read.page} in journal at pos ${pos}")
            val client = sender
            readFrom(journalFile, pos, journalHeader.pageSize)(read.haveRead(_, client))
          }.getOrElse {
            val pos = dataHeader.offsetForPage(read.page)
            val client = sender
            log.debug(s"Reading page ${read.page} from data at pos ${pos} for ${sender}")
            readFrom(dataFile, pos, dataHeader.pageSize)(read.haveRead(_, client))
          }  
        }
        
      case read @ HaveReadPage(_, replyTo) =>  
       replyTo ! ReadCompleted(read.value)
        
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
        
      case QueueEntryWritten(entry:WriteQueueEntry) =>
        entry.sender ! WriteCompleted
        entry.write.pages.keys.foreach(writing.remove)
        if (!writing.isEmpty) {
          log.warning("Writing log was not empty after completing a write. Concurrency bug.")
        }
        emptyWriteQueue()
        
      case Shutdown =>
        //log.debug("Shutdown request, syncing journal")
        emptyWriteQueue()
        sync()
        
      case SyncForShutdown =>
        //log.debug("Journal synced, poisining ourselves")
        self ! PoisonPill
        
      case create: Create[_] =>
        import create.pageType
        //TODO also update freelist with this page
        val page = pageCount
        self ? Write(page -> create.content) map (_ => CreateCompleted(page)) pipeTo sender
        pageCount += 1
      }
  }
}


object PagedStorage {
  trait PageType[T] {
    protected implicit val byteOrder = IO.byteOrder
    
    def fromByteString(page: ByteString): T
    def toByteString(page: T): ByteString
  }
  
  private case class HaveReadPage[T : PageType](bytes: ByteString, sender: ActorRef) {
    def value = implicitly[PageType[T]].fromByteString(bytes)
  }
  private case class WriteQueueEntry (sender: ActorRef, write: Write) {
    private var inProgress = write.pages.size
    def done = { inProgress <= 0 }
    def finishPage(page: PageIdx) {
      if (done) throw new Exception("Decremented more writes than we sent out...")
      inProgress -= 1;  
    }
  }
  
  private case class QueueEntryWritten(entry: WriteQueueEntry)
  private case class Creating (sender: ActorRef, response: CreateCompleted)
  private case object SyncForShutdown
    
  private[paged] case class InitialState(
      dataHeader: DataHeader, journalHeader: JournalHeader, 
      journalIndex: Map[PageIdx, Long], pageCount: PageIdx, journalPos: Long)
  
  case class Read[T: PageType] (page: PageIdx) {
    private[PagedStorage] def haveRead(bytes: ByteString, sender: ActorRef) = HaveReadPage(bytes, sender)
  }
  case class ReadCompleted[T] (content: T) 
 
  case class WriteContent[T: PageType](content: T, author: ActorRef) {
    def toByteString = implicitly[PageType[T]].toByteString(content) 
  } 
  case class Write private (pages: Map[PageIdx, WriteContent[_]]) {
    def +[T: PageType] (entry: (PageIdx, T))(implicit author: ActorRef) = {
      for (current <- pages.get(entry._1)) {
        if (current.author != author) throw new IllegalArgumentException(
          s"${author} Trying to overwrite page ${entry._1}, originally written by ${current.author}")
      }
      copy (pages = pages + (entry._1 -> WriteContent(entry._2, author)))  
    }
    lazy val pageBytes = pages.mapValues { _.toByteString }
  }
  object Write {
    def apply[T: PageType](entry: (PageIdx, T))(implicit author: ActorRef) = new Write(Map.empty) + entry
  }
  case object WriteCompleted

  case class Create[T <: AnyRef](content: T)(implicit val pageType: PageType[T]) 
  case class CreateCompleted(page: PageIdx)
  
  case object Shutdown
}
