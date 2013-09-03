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

//TODO rename to PagedStorage
class PagedFile(dataFile: ActorRef, journalFile: ActorRef, dataHeader: DataHeader, journalHeader: JournalHeader, 
                initialJournalIndex: Vector[PageIdx], initialPages: PageIdx) extends Actor with ActorLogging {

  val pageSize = dataHeader.pageSize
  var journalIndex = initialJournalIndex
  var pageCount = initialPages
  
  case class WriteQueueEntry (sender: ActorRef, write: Write)
  case class Reading(sender: ActorRef, read: Read)
  
  var writeQueue = Vector.empty[WriteQueueEntry]
  var writing = collection.mutable.Map.empty[PageIdx,ByteString]

  def receive = {
    case read:Read =>
      if (read.page >= pageCount) 
        throw new Exception (s"Trying to read page ${read.page} but only have ${pageCount}")
      writing.get(read.page).map { contentBeingWritten =>
        log.debug(s"Replying to ${sender}")
        sender ! ReadCompleted(contentBeingWritten, read.ctx)
      }.getOrElse {
        //performance: also keep a reverse journal Index of "latest page"
        val journalPage = journalIndex.lastIndexWhere(_ == read.page)
        if (journalPage != -1) {
          val pos = journalHeader.offsetForPageContent(PageIdx(journalPage))
          journalFile ! FileActor.Read(pos, journalHeader.pageSize, Reading(sender, read))
        } else {
          val pos = dataHeader.offsetForPage(read.page)
          dataFile ! FileActor.Read(pos, dataHeader.pageSize, Reading(sender, read))
        }
      }
      
    case FileActor.ReadCompleted(content, Reading(replyTo, read)) =>  
     replyTo ! ReadCompleted(content, read.ctx)
      
    case write:Write =>
      if (write.content.length > journalHeader.pageSize) 
        throw new Exception(s"Content length ${write.content.length} overflows page size ${journalHeader.pageSize}")
      val q = WriteQueueEntry(sender, write)
      if (writing.isEmpty) {
        performWrite(q)
      } else {
        writeQueue :+= q
      }
      
    case FileActor.WriteCompleted(WriteQueueEntry(replyTo, write)) =>
      replyTo ! WriteCompleted(write.ctx)
      writing.remove(write.page)
      if (writing.isEmpty) {
        emptyWriteQueue()
      }

    case other =>
      log.error("Dropping {}", other)
  }
  
  def performWrite(q: WriteQueueEntry) {
    val journalIdx = PageIdx(journalIndex.size)
    val pos = journalHeader.offsetForPageIdx(journalIdx)
    
    val pageIdxBytes = ByteString.newBuilder
    q.write.page.put(pageIdxBytes)(byteOrder)
    if (q.write.page >= pageCount) {
      pageCount = q.write.page + 1
    }
    
    val idxAndContent = pageIdxBytes.result ++ q.write.content
    
    journalFile ! FileActor.Write(pos, idxAndContent, q)
    writing(q.write.page) = q.write.content
    journalIndex :+= q.write.page
  }
  
  def emptyWriteQueue() {
    writeQueue.map(q => q.write.page -> q).toMap.values.foreach(performWrite)    
    writeQueue = Vector.empty
  }
  
}

object PagedFile {
  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN
  
  //TODO move away into StructuredPagedFile
  trait PageType[T <: AnyRef] {
    implicit val byteOrder = PagedFile.byteOrder
    
    def fromByteString(page: ByteString): T
    def toByteString(page: T): ByteString
  }
  
  def props (dataFile: ActorRef, journalFile: ActorRef, dataHeader: DataHeader, journalHeader: JournalHeader, 
                initialJournalIndex: Vector[PageIdx], initialPages: PageIdx) =
    Props(classOf[PagedFile], dataFile, journalFile, dataHeader, journalHeader, initialJournalIndex, initialPages.toInt)
  
  case class Read(page: PageIdx, ctx: AnyRef = None)
  case class ReadCompleted(content: ByteString, ctx: AnyRef)
  
  case class Write(page: PageIdx, content: ByteString, ctx: AnyRef = None)
  case class WriteCompleted(ctx: AnyRef)
  
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
    def pageCount(fileSize: Long): PageIdx = new PageIdx(((fileSize - DataHeader.size) / pageSize).toInt)
  }
  object DataHeader {
    private val defaultMagic = "lore-db4".getBytes("UTF-8")
    val size = defaultMagic.length + SizeOf.Int
    def apply(i: ByteIterator) = {
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
      PageIdx(((filesize - JournalHeader.size) / bytesPerPage).toInt)
    }
    def offsetForPageIdx(page: PageIdx) = JournalHeader.size + (page * bytesPerPage)
    def offsetForPageContent(page: PageIdx) = JournalHeader.size + (page * bytesPerPage) + SizeOf.PageIdx
  }
  object JournalHeader {
    private val defaultMagic = "lore-jr4".getBytes("UTF-8")
    val size = defaultMagic.length + SizeOf.Int    
    def apply(i: ByteIterator) = {
      val magic = new Array[Byte](defaultMagic.length)
      i.getBytes(magic)      
      new JournalHeader(magic, i.getInt)
    }
  }
  
  //TODO introduce StorageManager, so you can do IO(Storage) ? Open(...)
  class Opener(requestor: ActorRef, filename: String) extends Actor with ActorLogging {
    case object DataOpen
    case object JournalOpen
    case object ReadDataHeader
    case object ReadJournalHeader
    case object ReadJournalPage

    private var dataFile: ActorRef = _
    private var journalFile: ActorRef = _
    private val journalIndex = new VectorBuilder[PageIdx]
    private var journalPages = PageIdx(0)
    private var journalPageIdx = PageIdx(0)
    private var journalFileSize: Long = 0
    private var journalHeader: JournalHeader = _
    private var dataHeader: DataHeader = _
    private var isEmpty = false
    private var dataFileSize:Long = 0
    
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
        val pagedFile = context.system.actorOf(props( 
            dataFile, journalFile, dataHeader, journalHeader, journalIndex.result, dataHeader.pageCount(dataFileSize)))
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
        journalPages = journalHeader.pageCountWithFileSize(journalFileSize) 
        readJournal        
      }
    }
    
    private def handleEmptyDb {
      if (isEmpty && dataFile != null && journalFile != null) {
        dataFile ! FileActor.Write(0, new DataHeader().toByteString)
        dataFile ! FileActor.Sync()
        journalFile ! FileActor.Write(0, new JournalHeader().toByteString)
        journalFile ! FileActor.Sync()
        val pagedFile = context.system.actorOf(props( 
            dataFile, journalFile, DataHeader(), JournalHeader(), Vector.empty, PageIdx(0)))
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
  
}