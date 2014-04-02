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

class PagedStorageWorkerSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                       with WordSpecLike with Matchers with Eventually {
  implicit val byteStringPageType = new PagedStorage.PageType[ByteString] {
    def fromByteString(page: ByteString) = page
    def toByteString(page: ByteString) = page
    def empty = ByteString()
  }
  
  class Fixture(val initialPages:Int = 0, 
                val initialJournalIndex:Map[PageIdx, Long] = Map.empty)  {
    val dataFile = TestProbe()
    val journalFile = TestProbe()
    val opener = TestProbe()
    val dataHeader = DataHeader()
    val journalHeader = JournalHeader(dataHeader)
    val content = ByteString("Hello, world")
    val pageContent = content ++ ByteString(new Array[Byte](dataHeader.pageSize - content.size))
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
      //TODO move this to JournalEntrySpec
      write.bytes.size should be (dataHeader.pageSize + // content
                                  SizeOf.Int +          // number of pages (=1)
                                  SizeOf.PageIdx +      // page number
                                  SizeOf.MD5            // MD5
                                  )
      write.bytes.slice (0, 8).toList should be (1 :: 0 :: 0 :: 0 :: 
                                                 0 :: 0 :: 0 :: 0 :: Nil)
      
      journalFile.reply(FileActor.WriteCompleted)
      val haswritten = expectMsg(PagedStorage.WriteCompleted)
      
      f ! PagedStorage.Read[ByteString](PageIdx(0))
      val read = journalFile.expectMsgType[FileActor.Read]
      // after MD5 (16 bytes) + #pages (4 bytes) + pagenumber (4 bytes) 
      read.from should be (JournalHeader.size + 24) 
      read.size should be (dataHeader.pageSize)
      journalFile.reply(FileActor.ReadCompleted(pageContent))
      val hasread = expectMsgType[PagedStorage.ReadCompleted[ByteString]]
      hasread.content should be (pageContent)
    }
    
    "return content if it is stored in the journal" in new Fixture(
        initialPages = 1,
        initialJournalIndex = Map(PageIdx(0) -> 24)) {
      
      f ! PagedStorage.Read(PageIdx(0))
      val read = journalFile.expectMsgType[FileActor.Read]
      // should read from the value in initialJournalIndex
      read.from should be (24) 
      read.size should be (dataHeader.pageSize)
      journalFile.reply(FileActor.ReadCompleted(pageContent))
      val hasread = expectMsgType[PagedStorage.ReadCompleted[ByteString]]
      hasread.content should be (pageContent)      
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
      write.bytes.size should be (dataHeader.pageSize * 2 + // content of two pages
                                  SizeOf.Int +              // number of pages (=2)
                                  SizeOf.PageIdx +          // page number of page 1
                                  SizeOf.PageIdx +          // page number og page 2
                                  SizeOf.MD5                // MD5
                                  )
      write.bytes.slice (0, 12).toList should be (2 :: 0 :: 0 :: 0 :: 
                                                  0 :: 0 :: 0 :: 0 :: 
                                                  1 :: 0 :: 0 :: 0 :: Nil)
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
      val entry = JournalEntry(journalHeader, write2.bytes)
      entry.pages(PageIdx(1)).take(content2.length) should be (content2)
      
      journalFile.reply(FileActor.WriteCompleted)

      expectMsg(PagedStorage.WriteCompleted)
      expectMsg(PagedStorage.WriteCompleted)
      expectMsg(PagedStorage.WriteCompleted)
    }
    
    "return the latest version of a page that has been overwritten" in new Fixture {
      
    } 
  }

}