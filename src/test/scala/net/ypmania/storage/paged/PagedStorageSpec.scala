package net.ypmania.storage.paged

import java.io.File
import java.io.FileOutputStream

import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Random
import scala.util.Success
import scala.util.Try

import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import org.scalatest.concurrent.Eventually

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.testkit.ImplicitSender
import akka.testkit.TestKit
import akka.util.ByteString

class PagedStorageSpec extends TestKit(ActorSystem("Test")) with ImplicitSender with WordSpecLike with Matchers with Eventually {
  var openIdx = 0
  
  class Fixture {
    val filename = "/tmp/PagedFileSpec" + Random.nextInt
    val journalFilename = filename + ".j"
    val content = ByteString("Hello, world")
    
    def open() = {
      system.actorOf(Props(new Actor {
        override val supervisorStrategy = OneForOneStrategy() {
          case x =>
            testActor ! Failure(x)
            SupervisorStrategy.Escalate
        }
        
        val storage = context.actorOf(Props(classOf[PagedStorage], filename), "storage")
        def receive = {
          case PagedStorage.Ready => 
            testActor ! Success(storage)
        }
      }), "o$" + openIdx)
      openIdx += 1
      expectMsgType[Try[ActorRef]].get
    }
    
    def close(storage: ActorRef): Unit = {
      watch(storage)
      storage ! PagedStorage.Shutdown
      expectTerminated(storage, 2.seconds)      
    }
  }
  
  "a paged storage" should {
    "be able to create a new db and reopen it" in new Fixture {
      val storage = open()
      eventually {
        new File(filename).length should not be (0)
      }
      
      close(storage)
      new File(journalFilename).length should be (JournalHeader.size.toLong) 
      
      open()
    }
    
    "parse a journal with multiple entries" in new Fixture {
      val storage = open()
      storage ! PagedStorage.Write(Map(PageIdx(0) -> content))
      storage ! PagedStorage.Write(Map(PageIdx(1) -> content))
      expectMsgType[PagedStorage.WriteCompleted]
      expectMsgType[PagedStorage.WriteCompleted]
      
      close(storage)
      new File(journalFilename).length should be > JournalHeader.size.toLong 
      
      val reopened = open()
      
      reopened ! PagedStorage.Read(PageIdx(0))
      val page0 = expectMsgType[PagedStorage.ReadCompleted]
      page0.content.take(content.length) should be (content)
      
      reopened ! PagedStorage.Read(PageIdx(1))
      val page1 = expectMsgType[PagedStorage.ReadCompleted]
      page0.content.take(content.length) should be (content)
    }
    
    "be able to open a data file with missing journal" in new Fixture {
      close(open())
      new File(journalFilename).delete()
      close(open())
      eventually {
        new File(journalFilename).length should be (JournalHeader.size.toLong)         
      }
    }
    
    "be able to open a data file with zero-size journal" in new Fixture {
      close(open())
      new FileOutputStream(journalFilename).getChannel().truncate(0).force(true)
      close(open())
      eventually {        
        new File(journalFilename).length should be (JournalHeader.size.toLong)      
      }
    }
    
    "refuse to open a zero-size data" in new Fixture {
      new FileOutputStream(filename).getChannel().truncate(0).force(true)
      intercept[IllegalStateException] {
        open()
      }
    }
    
    "refuse to open a data file with wrong magic" in new Fixture {
      val out = new FileOutputStream(filename)
      out.write("Hello".getBytes())
      out.flush()
      out.close()
      
      intercept[IllegalStateException] {
        open()
      }
    }
    
    "refuse to open a data file with non-matching file size" in new Fixture {
      pending
    }
    
    "fail when data file can be created but journal can't" in new Fixture {
      pending
    }
    
    "truncate existing journal file when data file doesn't exist" in new Fixture {
      pending
    }
  }  
}