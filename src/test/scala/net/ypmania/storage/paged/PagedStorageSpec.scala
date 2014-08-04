package net.ypmania.storage.paged

import org.scalatest.Matchers

import org.scalatest.WordSpecLike
import org.scalatest.concurrent.Eventually

import net.ypmania.io.FileActor
import net.ypmania.io.IO._

import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.ImplicitSender
import akka.testkit.TestActorRef
import akka.testkit.TestKit
import akka.testkit.TestProbe
import akka.util.ByteString

import concurrent.duration._

class PagedStorageSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                       with WordSpecLike with Matchers with Eventually {
  implicit val byteStringPageType = new PagedStorage.PageType[ByteString] {
    def fromByteString(page: ByteString) = page
    def toByteString(page: ByteString) = page
    def empty = ByteString()
  }
  
  class Fixture(val initialPages:Int = 0, 
                val initialJournalIndex:Map[PageIdx, (Long,Int)] = Map.empty)  {
    val dataFile = TestProbe()
    val journalFile = TestProbe()
    val opener = TestProbe()
    val dataHeader = DataHeader()
    val journalHeader = JournalHeader(dataHeader)
    val content = ByteString("Hello, world")
    val pageContent = content
    val initialJournalPos:Long = JournalHeader.size
    
    val f = system.actorOf(Props(new PagedStorage("filename") {
      override def dataFileActor() = dataFile.ref
      override def journalFileActor() = journalFile.ref
      override def openerActor() = opener.ref
    }))
    
    f ! PagedStorage.InitialState(dataHeader, journalHeader, initialJournalIndex, PageIdx(initialPages), initialJournalPos)
  }
  
  "a paged file" should {
    "return empty results if reading outside of the file" in new Fixture {       
      f ! PagedStorage.Read(PageIdx(0))
      val hasread = expectMsgType[PagedStorage.ReadCompleted[ByteString]]
      hasread.content should be (ByteString())
    }
    
    "return written content while still writing it" in new Fixture {
      f ! PagedStorage.Write(PageIdx(0) -> content)
      val write = journalFile.expectMsgType[FileActor.Write]
      f ! PagedStorage.Read(PageIdx(0))
      val hasread = expectMsgType[PagedStorage.ReadCompleted[ByteString]]
      hasread.content should be (content)
      
      journalFile.reply(FileActor.WriteCompleted)
      val haswritten = expectMsg(PagedStorage.WriteCompleted)
    }
    
    "return content after storing it in the journal" in new Fixture {
      f ! PagedStorage.Write(PageIdx(0) -> content)
      val write = journalFile.expectMsgType[FileActor.Write]
      write.at should be (JournalHeader.size)
      
      journalFile.reply(FileActor.WriteCompleted)
      val haswritten = expectMsg(PagedStorage.WriteCompleted)
      
      f ! PagedStorage.Write(PageIdx(1) -> content)
      val write2 = journalFile.expectMsgType[FileActor.Write]
      write2.at should be (write.at + write.bytes.size)
      journalFile.reply(FileActor.WriteCompleted)
      val haswritten2 = expectMsg(PagedStorage.WriteCompleted)
      
      f ! PagedStorage.Read[ByteString](PageIdx(0))
      val read = journalFile.expectMsgType[FileActor.Read]
      val offset = 19 // expected offset within the original write where the actual page contents is stored
      read.from should be (write.at + offset)
      write.bytes.slice(offset, offset + content.length) should be (content)
      read.size should be (content.length) // only read the content that has actually be written.
      journalFile.reply(FileActor.ReadCompleted(pageContent))
      val hasread = expectMsgType[PagedStorage.ReadCompleted[ByteString]]
      hasread.content should be (pageContent)
      
      f ! PagedStorage.Read[ByteString](PageIdx(1))
      val read2 = journalFile.expectMsgType[FileActor.Read]
      read2.from should be (write2.at + offset)
    }
    
    "return content if it is stored in the journal" in new Fixture(
        initialPages = 2,
        initialJournalIndex = Map(PageIdx(0) -> (24, 200), PageIdx(1) -> (65572, 400))) {
      
      f ! PagedStorage.Read(PageIdx(0))
      val read = journalFile.expectMsgType[FileActor.Read]
      // should read from the value in initialJournalIndex
      read.from should be (24) 
      read.size should be (200)
      journalFile.reply(FileActor.ReadCompleted(pageContent))
      val hasread = expectMsgType[PagedStorage.ReadCompleted[ByteString]]
      hasread.content should be (pageContent)      
      
      f ! PagedStorage.Read(PageIdx(1))
      val read2 = journalFile.expectMsgType[FileActor.Read]
      read2.from should be (65572) 
    }
    
    "return content if it is stored in the data" in new Fixture(
        initialPages = 1) {
      
      f ! PagedStorage.Read(PageIdx(0))
      val read = dataFile.expectMsgType[FileActor.Read]
      read.from should be (DataHeader.size)
      read.size should be (dataHeader.pageSize)
      dataFile.reply(FileActor.ReadCompleted(pageContent))
      val hasread = expectMsgType[PagedStorage.ReadCompleted[ByteString]]
      hasread.content should be (pageContent)            
    }
    
    "write all pages of a multi-page write message" in new Fixture {
      f ! (PagedStorage.Write(PageIdx(0) -> content) + (PageIdx(1) -> content))
      val write = journalFile.expectMsgType[FileActor.Write]
      write.at should be (JournalHeader.size)
      
      val entry = JournalEntry(journalHeader, write.bytes.iterator)
      entry.pages should have size(2)
      entry.pages(PageIdx(0)) should be (content)
      entry.pages(PageIdx(1)) should be (content)
    }
    
    "combine queued-up writes to the same page, but send a reply for all" in new Fixture {
      val content2 = ByteString("Hello, again!")
      
      f ! (PagedStorage.Write(PageIdx(0) -> content))
      f ! (PagedStorage.Write(PageIdx(1) -> content))
      f ! (PagedStorage.Write(PageIdx(1) -> content2))
      
      val write1 = journalFile.expectMsgType[FileActor.Write]
      val sender1 = journalFile.sender
      journalFile.expectNoMsg(100.milliseconds)
      journalFile.send(sender1, FileActor.WriteCompleted)
      
      val write2 = journalFile.expectMsgType[FileActor.Write]
      val entry = JournalEntry(journalHeader, write2.bytes.iterator)
      entry.pages(PageIdx(1)).take(content2.length) should be (content2)
      
      journalFile.reply(FileActor.WriteCompleted)

      expectMsg(PagedStorage.WriteCompleted)
      expectMsg(PagedStorage.WriteCompleted)
      expectMsg(PagedStorage.WriteCompleted)
    }
    
    "not combine more than MAX_JOURNAL_ENTRY_SIZE writes" in new Fixture {
      pending
    }
    
    "return the latest version of a page that has been overwritten" in new Fixture {
      pending
    } 
    
    "reserve new pages into the next empty page number" in new Fixture {
      f ! (PagedStorage.Write(PageIdx(0) -> content) + (PageIdx(1) -> content))
      journalFile.expectMsgType[FileActor.Write]
      journalFile.reply(FileActor.WriteCompleted)
      expectMsg(PagedStorage.WriteCompleted)
      
      f ! PagedStorage.ReservePage
      f ! PagedStorage.ReservePage
      expectMsg(PagedStorage.PageReserved(PageIdx(2)))
      expectMsg(PagedStorage.PageReserved(PageIdx(3)))
    }
    
    "return metadata when asked" in new Fixture {
      f ! PagedStorage.GetMetadata
      expectMsgType[PagedStorage.Metadata] should be (PagedStorage.Metadata(dataHeader.pageSize, PageIdx(0)))
      
      f ! (PagedStorage.Write(PageIdx(0) -> content))
      journalFile.expectMsgType[FileActor.Write]
      journalFile.reply(FileActor.WriteCompleted)
      expectMsg(PagedStorage.WriteCompleted)
      
      f ! PagedStorage.GetMetadata
      expectMsgType[PagedStorage.Metadata] should be (PagedStorage.Metadata(dataHeader.pageSize, PageIdx(1)))
    }
    
    "stop itself if the dataFile actor dies" in new Fixture {
      watch(f)
      system.stop(dataFile.ref)
      expectTerminated(f, 1.second)
    }
    
    "stop itself if the journalFile actor dies" in new Fixture {
      watch(f)
      system.stop(journalFile.ref)
      expectTerminated(f, 1.second)
    }
    
    "stop itself if the opener actor dies" in new Fixture {
      watch(f)
      system.stop(opener.ref)
      expectTerminated(f, 1.second)      
    }
    
  }

}