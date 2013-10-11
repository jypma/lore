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

import PagedStorage._
class PagedStorage(dataFile: ActorRef, journalFile: ActorRef, dataHeader: DataHeader, journalHeader: JournalHeader, 
                   initialJournalIndex: Vector[PageIdx], initialPages: PageIdx) extends Actor with ActorLogging {

  val pageSize = dataHeader.pageSize
  var journalIndex = initialJournalIndex
  var pageCount = initialPages
  
  case class WriteQueueEntry (sender: ActorRef, write: Write) {
    private var inProgress = write.pages.size
    def done = { inProgress <= 0 }
    def finishPage(page: PageIdx) {
      if (done) throw new Exception("Decremented more writes than we sent out...")
      inProgress -= 1;  
    }
  }
  case class Reading(sender: ActorRef, read: Read)
  private case class Writing(entry: WriteQueueEntry, page: PageIdx)
  
  var writeQueue = Vector.empty[WriteQueueEntry]
  private var writing = collection.mutable.Map.empty[PageIdx, Writing]

  def receive = {
    case read:Read =>
      if (read.page >= pageCount) 
        throw new Exception (s"Trying to read page ${read.page} but only have ${pageCount}")
      writing.get(read.page).map { w =>
        log.debug(s"Replying in-transit write content to ${sender}")
        val contentBeingWritten = w.entry.write.pages(read.page)
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
      write.pages.foreach { case (page, content) =>
        if (content.length > journalHeader.pageSize) 
    	  throw new Exception(s"Content length ${content.length} for page ${page} overflows page size ${journalHeader.pageSize}")
      }
      val q = WriteQueueEntry(sender, write)
      if (writing.isEmpty) {
        performWrite(q)
      } else {
        writeQueue :+= q
      }
      
    case FileActor.WriteCompleted(Writing(entry, page)) =>
      entry.finishPage(page)
      if (entry.done) {
        entry.sender ! WriteCompleted(entry.write.ctx)
        entry.write.pages.keys.foreach(writing.remove)
        if (writing.isEmpty) {
          emptyWriteQueue()
        }
      }
      
    case other =>
      log.error("Dropping {}", other)
  }
  
  def performWrite(q: WriteQueueEntry) {
    for (page <- q.write.pages.keys) {
      performWrite(Writing(q, page))
    }
  }
  
  def performWrite(w: Writing) {
    val journalIdx = PageIdx(journalIndex.size)
    val pos = journalHeader.offsetForPageIdx(journalIdx)
    
    val pageIdxBytes = ByteString.newBuilder
    w.page.put(pageIdxBytes)(byteOrder)
    if (w.page >= pageCount) {
      pageCount = w.page + 1
    }
    
    val idxAndContent = pageIdxBytes.result ++ w.entry.write.pages(w.page)
    
    journalFile ! FileActor.Write(pos, idxAndContent, w)
    
    writing(w.page) = w
    journalIndex :+= w.page
  }
  
  def emptyWriteQueue() {
    writeQueue.foreach(performWrite)
    writeQueue = Vector.empty
  }
  
}

object PagedStorage {
  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN
  
  def props (dataFile: ActorRef, journalFile: ActorRef, dataHeader: DataHeader, journalHeader: JournalHeader, 
                initialJournalIndex: Vector[PageIdx], initialPages: PageIdx) =
    Props(classOf[PagedStorage], dataFile, journalFile, dataHeader, journalHeader, initialJournalIndex, initialPages.toInt)
  
  case class Read(page: PageIdx, ctx: AnyRef = None)
  case class ReadCompleted(content: ByteString, ctx: AnyRef)
  
  case class Write(pages: Map[PageIdx, ByteString], ctx: AnyRef = None)
  object Write {
    def apply(page: PageIdx, content: ByteString, ctx:AnyRef) = new Write(Map(page -> content), ctx)
    def apply(page: PageIdx, content: ByteString) = new Write(Map(page -> content), None)
  }
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
}