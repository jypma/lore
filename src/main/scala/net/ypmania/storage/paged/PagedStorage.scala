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
import net.ypmania.storage.atomic.AtomicActor

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
    var nextWrite:Option[Write] = None
    var nextWriteSenders = Seq.empty[ActorRef]
    var writing = Map.empty[PageIdx, Any]
  
    def performWrite(write: Write, senders: Seq[ActorRef]) {
      val journalEntry = JournalEntry(journalHeader, write.pageBytes)
      val content = journalEntry.toByteString
      journalFile ? FileActor.Write(journalPos, content) map {
        case FileActor.WriteCompleted => QueueEntryWritten(senders)
      } pipeTo self
      
      journalPos += SizeOf.MD5
      journalPos += SizeOf.Int // number of pages
      for (page <- write.pages.keys) {
        if (page >= pageCount) {
          pageCount = page + 1
        }
        writing += page -> write.pages(page).content
        
        journalPos += SizeOf.PageIdx
        log.debug(s"Stored page ${page} at ${journalPos}")
        journalIndex(page) = journalPos
        journalPos += journalHeader.pageSize
      }
    }
    
    def emptyWriteQueue() {
      for (write <- nextWrite) {
        performWrite(write, nextWriteSenders)
      }
      nextWrite = None
      nextWriteSenders = Seq.empty
      /*
      if (!writeQueue.isEmpty) {
        performWrite(writeQueue.map(_.write).reduce(MergeableWrite.merge), writeQueue.map(_.sender))        
      }
      writeQueue = Vector.empty
      */
    }
    
    def sync() {
      journalFile ? FileActor.Sync map { _ => SyncForShutdown} pipeTo self
    }
    
    context.become {
      case read:Read[_] =>
        log.debug(s"processing read(${read.page}) for ${sender}")
        writing.get(read.page).map { content =>
          log.debug(s"Replying in-transit write content to ${sender}")
          sender ! ReadCompleted(content)
        }.getOrElse {
          journalIndex.get(read.page).map { pos =>
            log.debug(s"Found page ${read.page} in journal at pos ${pos}")
            val client = sender
            readFrom(journalFile, pos, journalHeader.pageSize)(read.haveRead(_, client))
          }.getOrElse {
            if (read.page >= pageCount) {
              log.debug(s"Trying to read page ${read.page} but only have ${pageCount}. Returning empty.")
              sender ! ReadCompleted(read.emptyResult)
            } else {
              val pos = dataHeader.offsetForPage(read.page)
              val client = sender
              log.debug(s"Reading page ${read.page} from data at pos ${pos} for ${sender}")
              readFrom(dataFile, pos, dataHeader.pageSize)(read.haveRead(_, client))
            }
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

        if (writing.isEmpty) {
          performWrite(write, Seq(sender))
        } else {
          nextWrite = nextWrite.map(w => MergeableWrite.merge(w, write)).orElse(Some(write))
          nextWriteSenders :+= sender
        }
        
      case QueueEntryWritten(senders) =>
        senders foreach { _ ! WriteCompleted }
        writing = Map.empty
        emptyWriteQueue()
        
      case Shutdown =>
        //log.debug("Shutdown request, syncing journal")
        emptyWriteQueue()
        sync()
        
      case SyncForShutdown =>
        //log.debug("Journal synced, poisining ourselves")
        self ! PoisonPill
        
      case ReservePage =>
        //TODO also update freelist with this page
        val page = pageCount
        pageCount += 1
        sender ! PageReserved(page)
      }
  }
}


object PagedStorage {
  trait PageType[T] {
    def fromByteString(page: ByteString): T
    def toByteString(page: T): ByteString
    def empty: T
  }
  
  case class Read[T: PageType] (page: PageIdx) {
    def haveRead(bytes: ByteString, sender: ActorRef) = HaveReadPage(bytes, sender)
    def emptyResult = implicitly[PageType[T]].empty
  }
  case class ReadCompleted[T] (content: T) 
 
  case class WriteContent[T: PageType](content: T, author: ActorRef) {
    def toByteString = implicitly[PageType[T]].toByteString(content) 
  } 
  case class Write private (pages: Map[PageIdx, WriteContent[_]]) {
    def +[T: PageType] (entry: (PageIdx, T))(implicit author: ActorRef) = 
      plus (entry._1, WriteContent(entry._2, author))
    
    def ++(b: Write) = (this /: b.pages) { case (write, (p, c)) => write plus (p, c) }
      
    private[PagedStorage] def plus(p: PageIdx, c:WriteContent[_]) = {
      for (current <- pages.get(p)) {
        if (current.author != c.author) throw new IllegalArgumentException(
          s"${c.author} Trying to overwrite page ${p}, originally written by ${current.author}")
      }
      copy (pages = pages + (p -> c))        
    }
    lazy val pageBytes = pages.mapValues { _.toByteString }
  }
  implicit val MergeableWrite = new AtomicActor.Mergeable[Write] {
    def merge(a: Write, b: Write): Write = a ++ b
  }
  object Write {
    def apply[T: PageType](entry: (PageIdx, T))(implicit author: ActorRef) = new Write(Map.empty) + entry
  }
  case object WriteCompleted

  case object ReservePage 
  case class PageReserved(page: PageIdx)
  
  case object Shutdown
  
  case class HaveReadPage[T : PageType](bytes: ByteString, sender: ActorRef) {
    def value = implicitly[PageType[T]].fromByteString(bytes)
  }
  private case class WriteQueueEntry (sender: ActorRef, write: Write) 
  
  private case class QueueEntryWritten(senders: Seq[ActorRef])
  private case object SyncForShutdown
    
  private[paged] case class InitialState(
      dataHeader: DataHeader, journalHeader: JournalHeader, 
      journalIndex: Map[PageIdx, Long], pageCount: PageIdx, journalPos: Long)
}
