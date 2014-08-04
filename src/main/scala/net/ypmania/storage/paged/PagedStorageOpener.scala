package net.ypmania.storage.paged

import net.ypmania.io.FileActor
import net.ypmania.io.IO._
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.actor.Stash
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.ByteString
import scala.concurrent.duration._
import akka.util.Timeout

class PagedStorageOpener (client: ActorRef, dataFile: ActorRef, journalFile: ActorRef) extends Actor with ActorLogging {

  private implicit val timeout = Timeout(10.seconds)
  private implicit val executionContext = context.dispatcher
  
  def receive = { case _ => } 
  
  open()
  def open (): Unit = {
    dataFile ! FileActor.GetState
    var dataFileSize: Long = 0
    var dataHeader: DataHeader = null
        
    context.become {      
      case FileActor.New =>
        log.debug("Opened new data")
        dataHeader = DataHeader()
        dataFile ! FileActor.Write(0, dataHeader.toByteString)
        dataFile ! FileActor.Sync
        
      case FileActor.SyncCompleted =>
        createJournal(dataHeader, PageIdx(0))
  
      case FileActor.Existing(size) =>
        log.debug("Opened existing data of size {}", size)
        dataFileSize = size
        dataFile ! FileActor.Read(0, DataHeader.size)

      case FileActor.ReadCompleted(bytes) =>
        log.debug("Read data header with {} bytes", bytes.length)
        val dataHeader = DataHeader(bytes.iterator)
        if (!dataHeader.valid) {
          log.debug("data file invalid")
          throw new IllegalStateException("data file invalid")
        }
        val pageCount = dataHeader.pageCount(dataFileSize)
        readJournal(dataHeader, pageCount)
    }
  }
  
  def createJournal(dataHeader: DataHeader, pageCount: PageIdx): Unit = {
    log.debug(s"Creating journal with ${pageCount} pages")
    val journalHeader = JournalHeader(dataHeader)
    journalFile ! FileActor.Write(0, journalHeader.toByteString)
    journalFile ! FileActor.Sync
    
    client ! PagedStorage.InitialState(dataHeader, journalHeader, Map.empty, pageCount, JournalHeader.size)
    context become done
  }
  
  private def readFrom(file: ActorRef, pos: Long, size: Int)(conv: ByteString => AnyRef) {
    import FileActor._
    file ? Read(pos, size) map { case ReadCompleted(bytes) => conv(bytes) } pipeTo self    
  }
  
  def readJournal(dataHeader: DataHeader, dataPageCount: PageIdx): Unit = {
    case class ReadJournalHeader(bytes: ByteString)
    case class ReadJournalEntryIndex(bytes: ByteString)

    journalFile ! FileActor.GetState
    var journalHeader: JournalHeader = null
    var journalFileSize = 0l
    var journalPos = 0l
    val journalIndex = Map.newBuilder[PageIdx, (Long,Int)]
    var pageCount = dataPageCount
    
    def readNextJournalEntryIndex() {
      if (journalPos < journalFileSize) {
        // the size of a journal entry index should not be more than 16k
        readFrom(journalFile, journalPos, 16000)(ReadJournalEntryIndex)
      } else {
        log.debug("data file: {}", dataFile)
        log.debug("journal file: {}", journalFile)
        log.debug("page size: {}", dataHeader.pageSize)
        log.debug("journal index: {}", journalIndex.result)
        log.debug(s"Finished parsing. Journal pos ${journalPos} of ${journalFileSize}")
        
        client ! PagedStorage.InitialState(dataHeader, journalHeader, journalIndex.result, pageCount, journalPos)
      }
    }
    
    def validate {
      if (dataHeader.pageSize != journalHeader.pageSize) {
        log.debug("mismatched page size")
        throw new IllegalStateException(s"Data file page size ${dataHeader.pageSize} but journal has ${journalHeader.pageSize}")
      }
      readNextJournalEntryIndex()
    }

    def havePage(page: PageIdx) {
      pageCount = pageCount max (page + 1)
    }
    
    context.become {
      case FileActor.New =>
        log.debug("Opened new journal")
        createJournal(dataHeader, pageCount)

      case FileActor.Existing(size) =>
        log.debug("Opened existing journal of size {}", size)
        if (size < JournalHeader.size) {
          createJournal(dataHeader, pageCount)
        } else {
          journalFileSize = size;
          readFrom(journalFile, 0, JournalHeader.size)(ReadJournalHeader)
          journalPos = JournalHeader.size
        }

      case ReadJournalHeader(bytes) =>
        log.debug("Read journal header with {} bytes", bytes.length)
        journalHeader = JournalHeader(bytes.iterator)
        if (!journalHeader.valid) {
          log.debug("journal missing file magic")
          throw new IllegalStateException("journal missing file magic")
        }
        validate

      case ReadJournalEntryIndex(bytes) =>
        val i = bytes.iterator
        val entry = JournalEntryIndex(journalHeader, i)
        journalPos += entry.indexSize
        for ((page, length) <- entry.pageLengths) {
          journalIndex += (page -> (journalPos, length))
          journalPos += length
        }
        readNextJournalEntryIndex()
    }
  }
  
  def done: Receive = {
    case msg =>
      log.debug(s"Receiving ${msg} but we are already done")
  }
}
