package net.ypmania.lore

import org.scalatest.WordSpecLike
import org.scalatest.Matchers
import akka.actor.ActorSystem
import java.io.File
import akka.testkit.TestKit
import scala.util.Random
import akka.actor.ActorRef
import akka.actor.Props
import org.scalatest.concurrent.Eventually
import akka.testkit.TestProbe
import akka.actor.Status
import akka.testkit.TestActorRef
import akka.util.ByteString
import net.ypmania.io.FileActor
import akka.testkit.ImplicitSender

class PagedStorageSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                       with WordSpecLike with Matchers with Eventually {
  class Fixture(val initialPages:Int = 0, val initialJournalIndex:Vector[PageIdx] = Vector.empty[PageIdx])  {
    val dataFile = TestProbe()
    val journalFile = TestProbe()
    val dataHeader = PagedStorage.DataHeader()
    val journalHeader = PagedStorage.JournalHeader()
    val content = ByteString("Hello, world")
    val pageContent = content ++ ByteString(new Array[Byte](dataHeader.pageSize - content.size))
    
    val f = TestActorRef(PagedStorage.props(dataFile.ref, journalFile.ref, dataHeader,
        journalHeader, initialJournalIndex, PageIdx(initialPages)))
  }
  
  "a paged file" should {
    "not accept reads outside of the file" in new Fixture {        
      intercept[Exception] {
        f.receive(PagedStorage.Read(PageIdx(0)))        
      }
    }
    
    "return written content while still writing it" in new Fixture {
      f ! PagedStorage.Write(PageIdx(0), content)
      val write = journalFile.expectMsgType[FileActor.Write]
      f ! PagedStorage.Read(PageIdx(0))
      val hasread = expectMsgType[PagedStorage.ReadCompleted]
      hasread.content should be (content)
      
      journalFile.reply(FileActor.WriteCompleted(write.ctx))
      val haswritten = expectMsgType[PagedStorage.WriteCompleted]
    }
    
    "return content after it is stored in the journal" in new Fixture {
      f ! PagedStorage.Write(PageIdx(0), content)
      val write = journalFile.expectMsgType[FileActor.Write]
      // page 0 => right after the header
      write.at should be (PagedStorage.JournalHeader.size) 
      journalFile.reply(FileActor.WriteCompleted(write.ctx))
      val haswritten = expectMsgType[PagedStorage.WriteCompleted]
      
      f ! PagedStorage.Read(PageIdx(0))
      val read = journalFile.expectMsgType[FileActor.Read]
      // prefixed with page number
      read.from should be (write.at + 4) 
      read.size should be (dataHeader.pageSize)
      journalFile.reply(FileActor.ReadCompleted(pageContent, read.ctx))
      val hasread = expectMsgType[PagedStorage.ReadCompleted]
      hasread.content should be (pageContent)
    }
    
    "return content if it is stored in the journal" in new Fixture(
        initialPages = 1,
        initialJournalIndex = Vector(PageIdx(0))) {
      
      f ! PagedStorage.Read(PageIdx(0))
      val read = journalFile.expectMsgType[FileActor.Read]
      // prefixed with page number
      read.from should be (PagedStorage.JournalHeader.size + 4) 
      read.size should be (dataHeader.pageSize)
      journalFile.reply(FileActor.ReadCompleted(pageContent, read.ctx))
      val hasread = expectMsgType[PagedStorage.ReadCompleted]
      hasread.content should be (pageContent)      
    }
    
    "return content if it is stored in the data" in new Fixture(
        initialPages = 1) {
      
      f ! PagedStorage.Read(PageIdx(0))
      val read = dataFile.expectMsgType[FileActor.Read]
      read.from should be (PagedStorage.DataHeader.size)
      read.size should be (dataHeader.pageSize)
      dataFile.reply(FileActor.ReadCompleted(pageContent, read.ctx))
      val hasread = expectMsgType[PagedStorage.ReadCompleted]
      hasread.content should be (pageContent)            
    }
    
    "put writes into consecutive journal pages" in new Fixture {
      
    }
  }

}