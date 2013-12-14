package net.ypmania.io.paged;

import java.nio.file.Paths
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.WRITE
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.actorRef2Scala
import net.ypmania.io.FileActor
import net.ypmania.io.IO._
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy

trait PagedStorageOpener {
  this: Actor with ActorLogging with PagedStorageWorker =>
    
  case object ReadDataHeader
  case object ReadJournalHeader
  case object ReadJournalEntrySize
  case object ReadJournalEntryPages

  import PagedStorage._
  
  private var dataFile: ActorRef = _
  private var dataOpen = false
  private var journalFile: ActorRef = _
  private var journalOpen = false
  private val journalIndex = Map.newBuilder[PageIdx, Long]
  private var journalPos:Long = 0
  private var journalFileSize: Long = 0
  private var journalHeader: JournalHeader = _
  private var dataHeader: DataHeader = _
  private var isEmpty = false
  private var dataFileSize: Long = 0
  private var pageCount = PageIdx(0)

  override val supervisorStrategy = OneForOneStrategy() {
    case x:IllegalStateException => SupervisorStrategy.Escalate
  }
  
  def open (filename: String) {
    dataFile = context.actorOf(FileActor.props(
        Paths.get(filename), Seq(READ, WRITE, CREATE)), "d") 
    journalFile = context.actorOf(FileActor.props(
        Paths.get(filename + ".j"), Seq(READ, WRITE, CREATE)), "j")
    
  def readNextJournalEntrySize() {
    if (journalPos < journalFileSize) {
      journalFile ! FileActor.Read(journalPos, SizeOf.Int, ReadJournalEntrySize)
    } else {
      log.debug("data file: {}", dataFile)
      log.debug("journal file: {}", journalFile)
      log.debug("page size: {}", dataHeader.pageSize)
      log.debug("journal index: {}", journalIndex.result)
      log.debug(s"Finished parsing. Journal pos ${journalPos} of ${journalFileSize}")
      
      work(
        dataFile, journalFile, dataHeader, journalHeader, journalIndex.result, 
        pageCount, journalPos)
    }
  }

  def validate {
    if (dataHeader != null && journalHeader != null) {
      if (dataHeader.pageSize != journalHeader.pageSize) {
        log.debug("mismatched page size")
        throw new IllegalStateException(s"Data file page size ${dataHeader.pageSize} but journal has ${journalHeader.pageSize}")
      }
      readNextJournalEntrySize()
    }
  }

  def handleEmptyDb {
    if (isEmpty && dataOpen && journalOpen) {
      val dataHeader = DataHeader()
      dataFile ! FileActor.Write(0, dataHeader.toByteString)
      dataFile ! FileActor.Sync()
      val journalHeader = JournalHeader(dataHeader)
      journalFile ! FileActor.Write(0, journalHeader.toByteString)
      journalFile ! FileActor.Sync()
      work(
        dataFile, journalFile, dataHeader, journalHeader, Map.empty, PageIdx(0), 
        JournalHeader.size)
    }
  }

  def havePage(page: PageIdx) {
    pageCount = pageCount max (page + 1)
  }
  
    context.become {
      
    case FileActor.OpenedNew if sender == dataFile =>
      log.debug("Opened new data")
      dataOpen = true
      isEmpty = true
      handleEmptyDb

    case FileActor.OpenedExisting(size) if sender == dataFile =>
      log.debug("Opened existing data of size {}", size)
      dataOpen = true
      dataFileSize = size
      dataFile ! FileActor.Read(0, DataHeader.size, ReadDataHeader)

    case FileActor.ReadCompleted(bytes, ReadDataHeader) =>
      log.debug("Read data header with {} bytes", bytes.length)
      dataHeader = DataHeader(bytes.iterator)
      havePage(dataHeader.pageCount(dataFileSize))
      if (!dataHeader.valid) {
        log.debug("data file invalid")
        throw new IllegalStateException("data file invalid")
      }
      validate

    case FileActor.OpenedNew if sender == journalFile =>
      journalOpen = true
      log.debug("Opened new journal")
      handleEmptyDb

    case FileActor.OpenedExisting(size) if sender == journalFile =>
      journalOpen = true
      log.debug("Opened existing journal of size {}", size)
      handleEmptyDb
      journalFileSize = size;
      journalFile ! FileActor.Read(0, JournalHeader.size, ReadJournalHeader)
      journalPos = JournalHeader.size

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

    case other =>
      log.error(s"Opener dropping ${other} from ${sender}")
  }
  }
}