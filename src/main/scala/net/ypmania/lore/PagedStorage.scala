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
import java.security.MessageDigest

class PagedStorage(dataFile: ActorRef, journalFile: ActorRef, dataHeader: DataHeader, journalHeader: JournalHeader, 
                   initialJournalIndex: Map[PageIdx, Long], initialPages: PageIdx, initialJournalPos: Long) extends Actor with ActorLogging {

  implicit val byteOrder = PagedStorage.byteOrder
  
  var journalIndex = collection.mutable.Map.empty[PageIdx,Long] ++ initialJournalIndex
  var journalPos = initialJournalPos
  var pageCount = initialPages
  var writeQueue = Vector.empty[WriteQueueEntry]
  var writing = collection.mutable.Map.empty[PageIdx, WriteQueueEntry]

  val emptyPageArray = new Array[Byte](dataHeader.pageSize)
  val emptyPage = ByteString(emptyPageArray)
  
  case class JournalEntry private(pages: Map[PageIdx, ByteString], md5: ByteString) {
    def toByteString = {
      val bs = new ByteStringBuilder
      bs ++= md5
      bs.putInt(pages.size)
      for (pageIdx <- pages.keys) { 
        pageIdx.put(bs)
      }
      for (content <- pages.values) {
        bs ++= content
        if (content.size < dataHeader.pageSize) {
          // Fill with zeroes
          bs ++= emptyPage.take(dataHeader.pageSize - content.size)
        }
      }
      bs.result
    }    
  }
  object JournalEntry {
    def apply(pages: Map[PageIdx, ByteString]) = {
      val md = MessageDigest.getInstance("MD5")
      for (pageIdx <- pages.keys) {
        val b = new ByteStringBuilder()
        pageIdx.put(b)(PagedStorage.byteOrder)
        md.update(b.result.asByteBuffer)
      }
      for (content <- pages.values) {
        md.update(content.asByteBuffer)
        if (content.size < dataHeader.pageSize) {
          // Fill with zeroes
          md.update(emptyPageArray, 0, dataHeader.pageSize - content.size)
        }
      }
      new JournalEntry(pages, ByteString(md.digest()))
    }
  }
  
  case class WriteQueueEntry (sender: ActorRef, write: Write) {
    private var inProgress = write.pages.size
    def done = { inProgress <= 0 }
    def finishPage(page: PageIdx) {
      if (done) throw new Exception("Decremented more writes than we sent out...")
      inProgress -= 1;  
    }
  }
  case class Reading(sender: ActorRef, read: Read)
  case class Writing(entry: WriteQueueEntry, page: PageIdx)
  
  def receive = {
    case read:Read =>
      if (read.page >= pageCount) 
        throw new Exception (s"Trying to read page ${read.page} but only have ${pageCount}")
      writing.get(read.page).map { entry =>
        log.debug(s"Replying in-transit write content to ${sender}")
        val contentBeingWritten = entry.write.pages(read.page)
        sender ! ReadCompleted(contentBeingWritten, read.ctx)
      }.getOrElse {
        journalIndex.get(read.page).map { pos =>
          log.debug(s"Found page ${read.page} in journal at pos ${pos}")
          journalFile ! FileActor.Read(pos, journalHeader.pageSize, Reading(sender, read))
        }.getOrElse {
          val pos = dataHeader.offsetForPage(read.page)
          log.debug(s"Reading page ${read.page} from data at pos ${pos}")
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
      
    case FileActor.WriteCompleted(entry:WriteQueueEntry) =>
      entry.sender ! WriteCompleted(entry.write.ctx)
      entry.write.pages.keys.foreach(writing.remove)
      if (!writing.isEmpty) {
        log.warning("Writing log was not empty after completing a write. Concurrency bug.")
      }
      emptyWriteQueue()
      
    case other =>
      log.error("Dropping {}", other)
  }
  
  def performWrite(q: WriteQueueEntry) {
    val journalEntry = JournalEntry(q.write.pages)
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
  
}

object PagedStorage {
  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN
  
  def props (dataFile: ActorRef, journalFile: ActorRef, dataHeader: DataHeader, journalHeader: JournalHeader, 
                initialJournalIndex: Map[PageIdx, Long], initialPages: PageIdx, initialJournalPos: Long) =
    Props(classOf[PagedStorage], dataFile, journalFile, dataHeader, journalHeader, initialJournalIndex, initialPages.toInt, initialJournalPos)
  
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