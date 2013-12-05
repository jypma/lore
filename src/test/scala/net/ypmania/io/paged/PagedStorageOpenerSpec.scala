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

class PagedFileOpenerSpec extends TestKit(ActorSystem("Test")) with WordSpecLike with Matchers with Eventually {
  class Fixture {
    val filename = "/tmp/PagedFileSpec" + Random.nextInt
  }
  
  "a paged file opener" should {
    "be able to open a cleanly created new db" in new Fixture {
      system.actorOf(Props(classOf[PagedStorageOpener], testActor, filename))
      expectMsgType[ActorRef]
      eventually {
        new File(filename).length should not be (0)
      }
      // TODO write some pages
      
      system.actorOf(Props(classOf[PagedStorageOpener], testActor, filename))
      val createdFile = expectMsgType[ActorRef]
      new File(filename) should be ('exists)      
    }
  }  
}