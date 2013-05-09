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
import PagedFile._
import akka.actor.PoisonPill
import akka.util.ByteStringBuilder
import akka.actor.ActorLogging

class PagedFile(dataFile: ActorRef, journalFile: ActorRef, dataHeader: DataHeader, journalHeader: JournalHeader, 
                initialJournalIndex: Vector[PageIdx], initialPages: PageIdx) extends Actor {
  import BinarySearch._
  val pageSize = dataHeader.pageSize
  var journalIndex = initialJournalIndex
  var pageCount = initialPages
  
  def receive = {
    case read:Read[_] =>
      val replyTo = sender
      context.actorOf(Props(new Reader(replyTo, read)))
  }
  
  class Reader(requestor: ActorRef, read: Read[_]) extends Actor with ActorLogging {
    override def preStart {
      val pos = journalIndex.binarySearch(read.page)
      if (pos > 0) {
        journalFile ! FileActor.Read(journalHeader.offsetForPageContent(pos), pageSize)
      } else {
        if (read.page >= pageCount) {
          throw new Exception (s"Trying to read page ${read.page} but have only ${pageCount}")
        } else {
          dataFile ! FileActor.Read(dataHeader.offsetForPage(read.page), pageSize)
        }
      }
    }
    
    def receive = {
      case FileActor.ReadCompleted(bytes, _) =>
        requestor ! read.pageType.read(bytes)
      case other =>
        log.error("Dropping: {}", other)
        
    }
  }
}

object PagedFile {
  type PageIdx = Int
  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN
  
  trait PageType[T] {
    def read(page: ByteString): T
    def write(page: T): ByteString
  }
  
  case class Read[T](page: PageIdx, pageType: PageType[T])
  
  val defaultPageSize = 64 * 1024
  
  case class DataHeader(magic: Array[Byte] = DataHeader.defaultMagic, 
                        pageSize: Int = defaultPageSize) {
    def valid = magic.sameElements(DataHeader.defaultMagic)
    def toByteString = {
      val bs = new ByteStringBuilder
      bs.putBytes(magic)
      bs.putInt(pageSize)
      bs.result
    }
    def offsetForPage(page: PageIdx) = DataHeader.size + (page * pageSize)
    def pageCount(fileSize: Long): PageIdx = ((fileSize - DataHeader.size) / pageSize).toInt
  }
  object DataHeader {
    private val defaultMagic = "lore-db4".getBytes("UTF-8")
    val size = defaultMagic.length + SizeOf.Int
    def read(i: ByteIterator) = {
      val magic = new Array[Byte](defaultMagic.length)
      i.getBytes(magic)      
      new DataHeader(magic, i.getInt)
    }
  }
  
  case class JournalHeader(magic: Array[Byte] = JournalHeader.defaultMagic,
                           pageSize: Int = defaultPageSize) {
    def valid = magic.sameElements(JournalHeader.defaultMagic)
    def toByteString = {
      val bs = new ByteStringBuilder
      bs.putBytes(magic)
      bs.putInt(pageSize)
      bs.result
    }
    def bytesPerPage = SizeOf.PageIdx + pageSize
    def pageCountWithFileSize(filesize: Long) = {
      if ((filesize - JournalHeader.size) % (pageSize + SizeOf.PageIdx) != 0) {
        println("corrupt journal file size")
        throw new Exception ("Corrupt journal file size: " + filesize)
      }
      (filesize - JournalHeader.size) / bytesPerPage
    }
    def offsetForPageIdx(page: Int) = JournalHeader.size + (page * bytesPerPage)
    def offsetForPageContent(page: Int) = JournalHeader.size + (page * bytesPerPage) + SizeOf.PageIdx
  }
  object JournalHeader {
    private val defaultMagic = "lore-jr4".getBytes("UTF-8")
    val size = defaultMagic.length + SizeOf.Int    
    def read(i: ByteIterator) = {
      val magic = new Array[Byte](defaultMagic.length)
      i.getBytes(magic)      
      new JournalHeader(magic, i.getInt)
    }
  }
  
  class Opener(requestor: ActorRef, filename: String) extends Actor with ActorLogging {
    case object DataOpen
    case object JournalOpen
    case object ReadDataHeader
    case object ReadJournalHeader
    case object ReadJournalPage

    private var dataFile: ActorRef = _
    private var journalFile: ActorRef = _
    private val journalIndex = new VectorBuilder[PageIdx]
    private var journalPages: Long = 0
    private var journalPageIdx: PageIdx = 0
    private var journalFileSize: Long = 0
    private var journalHeader: JournalHeader = _
    private var dataHeader: DataHeader = _
    private var isEmpty = false
    private var dataFileSize:Long = 0
    
    override def preStart {
      val io = context.actorOf(Props(new FileActor.IO))
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
        val pagedFile = context.system.actorOf(Props(new PagedFile(
            dataFile, journalFile, dataHeader, journalHeader, journalIndex.result, dataHeader.pageCount(dataFileSize))))
        requestor ! pagedFile
        context.stop(self)
      }
    }
    
    private def validate {
      if (dataHeader != null && journalHeader != null) {
        if (dataHeader.pageSize != journalHeader.pageSize) {
          log.debug("mismatched page size")
          throw new Exception (s"Data file page size ${dataHeader.pageSize} but journal has ${journalHeader.pageSize}")
        }
      }
    }
    
    private def handleEmptyDb {
      if (isEmpty && dataFile != null && journalFile != null) {
        dataFile ! FileActor.Write(0, new DataHeader().toByteString)
        dataFile ! FileActor.Sync()
        journalFile ! FileActor.Write(0, new JournalHeader().toByteString)
        journalFile ! FileActor.Sync()
        val pagedFile = context.system.actorOf(Props(new PagedFile(
            dataFile, journalFile, DataHeader(), JournalHeader(), Vector.empty, 0)))
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
        dataHeader = DataHeader.read(bytes.iterator)
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
        journalHeader = JournalHeader.read(bytes.iterator)
        if (!journalHeader.valid) {
          log.debug("journal missing file magic")
          throw new Exception("journal missing file magic")
        }
        validate
        journalPages = journalHeader.pageCountWithFileSize(journalFileSize) 
        readJournal
      
      case FileActor.ReadCompleted(bytes, ReadJournalPage) =>
        log.debug("Read journal page")
        journalIndex += bytes.iterator.getInt
        journalPageIdx += 1
        readJournal
        
      case other =>
        log.error("Dropping {}", other)
    }
  }
  
}