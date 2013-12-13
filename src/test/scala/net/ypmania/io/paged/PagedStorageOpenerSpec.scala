package net.ypmania.io.paged

import org.scalatest.WordSpecLike
import org.scalatest.Matchers
import akka.actor.ActorSystem
import akka.testkit.TestKit
import scala.util.Random
import org.scalatest.concurrent.Eventually
import akka.actor.ActorRef
import akka.actor.Props
import java.io.File
import concurrent.duration._
import language.postfixOps
import akka.util.ByteString
import akka.actor.PoisonPill
import akka.testkit.TestActorRef
import akka.testkit.ImplicitSender

class PagedFileOpenerSpec extends TestKit(ActorSystem("Test")) with ImplicitSender with WordSpecLike with Matchers with Eventually {
  class Fixture {
    val filename = "/tmp/PagedFileSpec" + Random.nextInt
    val journalFilename = filename + ".j"
    val content = ByteString("Hello, world")
    
    def open() = {
      val opener = system.actorOf(Props(classOf[PagedStorageOpener], testActor, filename), "opener")
      println("Watching opener")
      val storage = expectMsgType[ActorRef]
      
      watch(opener)
      expectTerminated(opener, 1 second)
      println("Opener terminated")
      storage
    }
  }
  
  "a paged file opener" should {
    "be able to open a cleanly created new db" in new Fixture {
      val storage = open()
      eventually {
        new File(filename).length should not be (0)
      }
      
      watch(storage)
      storage ! PagedStorage.Shutdown
      expectTerminated(storage, 2 seconds)
      new File(journalFilename).length should be (JournalHeader.size.toLong) 
      
      open()
    }
    
    "parse a journal with multiple entries" in new Fixture {
      val storage = open()
      storage ! PagedStorage.Write(Map(PageIdx(0) -> content))
      storage ! PagedStorage.Write(Map(PageIdx(1) -> content))
      expectMsgType[PagedStorage.WriteCompleted]
      expectMsgType[PagedStorage.WriteCompleted]
      
      watch(storage)
      storage ! PagedStorage.Shutdown
      expectTerminated(storage, 2 seconds)
      new File(journalFilename).length should be > JournalHeader.size.toLong 
      
      val reopened = open()
      
      reopened ! PagedStorage.Read(PageIdx(0))
      val page0 = expectMsgType[PagedStorage.ReadCompleted]
      page0.content.take(content.length) should be (content)
      
      reopened ! PagedStorage.Read(PageIdx(1))
      val page1 = expectMsgType[PagedStorage.ReadCompleted]
      page0.content.take(content.length) should be (content)
    }
  }  
}