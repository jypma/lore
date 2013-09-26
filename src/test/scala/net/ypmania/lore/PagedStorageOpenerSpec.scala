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

class PagedFileOpenerSpec extends TestKit(ActorSystem("Test")) with WordSpecLike with Matchers with Eventually {
  class Fixture {
    val filename = "/tmp/PagedFileSpec" + Random.nextInt
  }
  
  "a paged file opener" should {
    "be able to open a cleanly created new db" in new Fixture {
      system.actorOf(Props(classOf[PagedStorage.Opener], testActor, filename))
      expectMsgType[ActorRef]
      eventually {
        new File(filename).length should not be (0)
      }
      system.actorOf(Props(classOf[PagedStorage.Opener], testActor, filename))
      val createdFile = expectMsgType[ActorRef]
      new File(filename) should be ('exists)      
    }
  }  
}