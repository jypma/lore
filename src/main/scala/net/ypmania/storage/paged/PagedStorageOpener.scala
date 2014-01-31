package net.ypmania.storage.paged

import net.ypmania.io.FileActor
import net.ypmania.io.IO._
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.actor.Stash

class PagedStorageOpener (client: ActorRef, dataFile: ActorRef, journalFile: ActorRef) extends Actor with ActorLogging {

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
        dataFile ! FileActor.Sync()
        
      case FileActor.SyncCompleted(_) =>
        createJournal(dataHeader, PageIdx(0))
  
      case FileActor.Existing(size) =>
        log.debug("Opened existing data of size {}", size)
        dataFileSize = size
        dataFile ! FileActor.Read(0, DataHeader.size)

      case FileActor.ReadCompleted(bytes, _) =>
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
    val journalHeader = JournalHeader(dataHeader)
    journalFile ! FileActor.Write(0, journalHeader.toByteString)
    journalFile ! FileActor.Sync()
    
    client ! PagedStorage.InitialState(dataHeader, journalHeader, Map.empty, pageCount, JournalHeader.size)
  }
    
  def readJournal(dataHeader: DataHeader, dataPageCount: PageIdx): Unit = {
    case object ReadJournalHeader
    case object ReadJournalEntrySize
    case object ReadJournalEntryPages

    journalFile ! FileActor.GetState
    var journalHeader: JournalHeader = null
    var journalFileSize = 0l
    var journalPos = 0l
    val journalIndex = Map.newBuilder[PageIdx, Long]
    var pageCount = dataPageCount
    
    def readNextJournalEntrySize() {
      if (journalPos < journalFileSize) {
        journalFile ! FileActor.Read(journalPos, SizeOf.Int, ReadJournalEntrySize)
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
      readNextJournalEntrySize()
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
          journalFile ! FileActor.Read(0, JournalHeader.size, ReadJournalHeader)
          journalPos = JournalHeader.size
        }

      case FileActor.ReadCompleted(bytes, ReadJournalHeader) =>
        log.debug("Read journal header with {} bytes", bytes.length)
        journalHeader = JournalHeader(bytes.iterator)
        if (!journalHeader.valid) {
          log.debug("journal missing file magic")
          throw new IllegalStateException("journal missing file magic")
        }
        validate

      case FileActor.ReadCompleted(bytes, ReadJournalEntrySize) =>
        val pageCount = bytes.iterator.getInt
        log.debug(s"Read journal entry with ${pageCount} pages")
        journalPos += SizeOf.Int
        journalFile ! FileActor.Read(journalPos, SizeOf.PageIdx * pageCount, ReadJournalEntryPages)
  
      case FileActor.ReadCompleted(bytes, ReadJournalEntryPages) =>
        journalPos += bytes.size
        val entryPageCount = bytes.size / SizeOf.PageIdx
        log.debug(s"Parsing journal entry with ${entryPageCount} pages")
        val iterator = bytes.iterator
        for (i <- 0 until entryPageCount) {
          val pageIdx = PageIdx.get(iterator)
          havePage(pageIdx)
          log.debug(s"Page ${pageIdx} is at position ${journalPos}")
          journalIndex += (pageIdx -> journalPos)
          journalPos += journalHeader.pageSize
        }
        journalPos += SizeOf.MD5
        readNextJournalEntrySize()
    }
  }
}
