package net.ypmania.lore

import java.nio.file.Paths
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.WRITE
import akka.actor.Actor
import akka.actor.Props
import net.ypmania.io.FileActor
import akka.actor.ActorRef
import java.nio.ByteOrder
import akka.actor.Status
import akka.util.ByteString
import scala.collection.immutable.VectorBuilder
import net.ypmania.io.IO._
import akka.util.ByteIterator
import akka.actor.PoisonPill
import akka.util.ByteStringBuilder
import akka.actor.ActorLogging

class PagedStorageOpener(requestor: ActorRef, filename: String) extends Actor with ActorLogging {
  case object DataOpen
  case object JournalOpen
  case object ReadDataHeader
  case object ReadJournalHeader
  case object ReadJournalPage

  import PagedStorage._
  
  private var dataFile: ActorRef = _
  private var journalFile: ActorRef = _
  private val journalIndex = new VectorBuilder[PageIdx]
  private var journalPages = PageIdx(0)
  private var journalPageIdx = PageIdx(0)
  private var journalFileSize: Long = 0
  private var journalHeader: JournalHeader = _
  private var dataHeader: DataHeader = _
  private var isEmpty = false
  private var dataFileSize: Long = 0

  override def preStart {
    val io = context.actorOf(Props[FileActor.IO])
    io ! FileActor.IO.Open(Paths.get(filename), Seq(READ, WRITE, CREATE), DataOpen)
    io ! FileActor.IO.Open(Paths.get(filename + ".j"), Seq(READ, WRITE, CREATE), JournalOpen)
  }

  private def readJournal {
    if (journalPageIdx < journalPages) {
      journalFile ! FileActor.Read(journalHeader.offsetForPageIdx(journalPageIdx), SizeOf.PageIdx, ReadJournalPage)
    } else {
      log.debug("data file: {}", dataFile)
      log.debug("journal file: {}", journalFile)
      log.debug("page size: {}", dataHeader.pageSize)
      log.debug("journal index: {}", journalIndex.result)
      val pagedStorage = context.system.actorOf(PagedStorage.props(
        dataFile, journalFile, dataHeader, journalHeader, journalIndex.result, dataHeader.pageCount(dataFileSize)))
      requestor ! pagedStorage
      context.stop(self)
    }
  }

  private def validate {
    if (dataHeader != null && journalHeader != null) {
      if (dataHeader.pageSize != journalHeader.pageSize) {
        log.debug("mismatched page size")
        throw new Exception(s"Data file page size ${dataHeader.pageSize} but journal has ${journalHeader.pageSize}")
      }
      journalPages = journalHeader.pageCountWithFileSize(journalFileSize)
      readJournal
    }
  }

  private def handleEmptyDb {
    if (isEmpty && dataFile != null && journalFile != null) {
      val dataHeader = DataHeader()
      dataFile ! FileActor.Write(0, dataHeader.toByteString)
      dataFile ! FileActor.Sync()
      val journalHeader = JournalHeader()
      journalFile ! FileActor.Write(0, journalHeader.toByteString)
      journalFile ! FileActor.Sync()
      val pagedFile = context.system.actorOf(PagedStorage.props(
        dataFile, journalFile, dataHeader, journalHeader, Vector.empty, PageIdx(0)))
      requestor ! pagedFile
      context.stop(self)
    }
  }

  def receive = {
    case FileActor.IO.OpenedNew(file, DataOpen) =>
      log.debug("Opened new data")
      dataFile = file
      isEmpty = true
      handleEmptyDb

    case FileActor.IO.OpenedExisting(file, size, DataOpen) =>
      log.debug("Opened existing data of size {}", size)
      dataFile = file
      dataFile ! FileActor.Read(0, DataHeader.size, ReadDataHeader)
      dataFileSize = size

    case FileActor.ReadCompleted(bytes, ReadDataHeader) =>
      log.debug("Read data header with {} bytes", bytes.length)
      dataHeader = DataHeader(bytes.iterator)
      if (!dataHeader.valid) {
        log.debug("data file invalid")
        throw new Exception("data file invalid")
      }
      validate

    case FileActor.IO.OpenedNew(file, JournalOpen) =>
      log.debug("Opened new journal")
      journalFile = file
      handleEmptyDb

    case FileActor.IO.OpenedExisting(file, size, JournalOpen) =>
      log.debug("Opened existing journal of size {}", size)
      journalFile = file;
      journalFileSize = size;
      journalFile ! FileActor.Read(0, JournalHeader.size, ReadJournalHeader)
      handleEmptyDb

    case FileActor.ReadCompleted(bytes, ReadJournalHeader) =>
      log.debug("Read journal header with {} bytes", bytes.length)
      journalHeader = JournalHeader(bytes.iterator)
      if (!journalHeader.valid) {
        log.debug("journal missing file magic")
        throw new Exception("journal missing file magic")
      }
      validate

    case FileActor.ReadCompleted(bytes, ReadJournalPage) =>
      log.debug("Read journal page")
      journalIndex += PageIdx.get(bytes.iterator)
      journalPageIdx += 1
      readJournal

    case other =>
      log.error("Dropping {}", other)
  }
}