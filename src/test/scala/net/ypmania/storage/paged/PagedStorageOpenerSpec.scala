package net.ypmania.storage.paged

import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.concurrent.Eventually
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.TestProbe
import net.ypmania.io.FileActor
import scala.concurrent.duration._
import akka.testkit.TestActor.AutoPilot
import akka.actor.ActorRef
import akka.util.ByteString

class PagedStorageOpenerSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                       with WordSpecLike with Matchers with Eventually {
  trait Fixture {
    val dataFile = TestProbe()
    val journalFile = TestProbe()
    val f = system.actorOf(Props(new PagedStorageOpener(self, dataFile.ref, journalFile.ref)))
  }
  
  "A PagedStorageOpener" should {
    "be able to open a data file with missing journal, creating the journal" in new Fixture {
      val dataHeader = DataHeader()
      dataFile.expectMsg(FileActor.GetState)
      dataFile.reply(FileActor.Existing(DataHeader.size))
      dataFile.expectMsgType[FileActor.Read]
      dataFile.reply(FileActor.ReadCompleted(dataHeader.toByteString))
      journalFile.expectMsg(FileActor.GetState)
      journalFile.reply(FileActor.New)
      val write = journalFile.expectMsgType[FileActor.Write]
      write.at should be (0)
      write.bytes should be (JournalHeader(dataHeader).toByteString)
      journalFile.expectMsg(FileActor.Sync)
      val state = expectMsgType[PagedStorage.InitialState]
      state.dataHeader should be (dataHeader)
      state.journalHeader  should be (JournalHeader(dataHeader))
      state.pageCount should be (PageIdx(0))
      state.journalPos should be (JournalHeader.size)
    }
    
    "be able to open a data file with zero-size journal, creating the journal" in new Fixture {
      val dataHeader = DataHeader()
      dataFile.expectMsg(FileActor.GetState)
      dataFile.reply(FileActor.Existing(DataHeader.size))
      dataFile.expectMsgType[FileActor.Read]
      dataFile.reply(FileActor.ReadCompleted(dataHeader.toByteString))
      journalFile.expectMsg(FileActor.GetState)
      journalFile.reply(FileActor.Existing(0))
      val write = journalFile.expectMsgType[FileActor.Write]
      write.at should be (0)
      write.bytes should be (JournalHeader(dataHeader).toByteString)
      journalFile.expectMsg(FileActor.Sync)
      val state = expectMsgType[PagedStorage.InitialState]
      state.dataHeader should be (dataHeader)
      state.journalHeader  should be (JournalHeader(dataHeader))
      state.pageCount should be (PageIdx(0))
      state.journalPos should be (JournalHeader.size)
    }
    
    "refuse to open a data file with wrong magic" in new Fixture {
      watch(f)
      dataFile.expectMsg(FileActor.GetState)
      dataFile.reply(FileActor.Existing(DataHeader.size))
      dataFile.expectMsgType[FileActor.Read]
      dataFile.reply(FileActor.ReadCompleted(ByteString("                        ")))
      expectTerminated(f, 1.second)
    }
  }
}