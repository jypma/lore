package net.ypmania.lore

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import akka.actor.ActorSystem
import java.io.File
import akka.testkit.TestKit
import scala.util.Random
import akka.actor.ActorRef
import akka.actor.Props
import org.scalatest.concurrent.Eventually

class PagedFileOpenerSpec extends TestKit(ActorSystem("Test")) with WordSpec with ShouldMatchers with Eventually {
  class Fixture {
    val filename = "/tmp/PagedFileSpec" + Random.nextInt
  }
  
  "a paged file opener" should {
    "be able to open a cleanly created new db" in new Fixture {
      system.actorOf(Props(classOf[PagedFile.Opener], testActor, filename))
      expectMsgType[ActorRef]
      eventually {
        new File(filename).length should not be (0)
      }
      system.actorOf(Props(classOf[PagedFile.Opener], testActor, filename))
      val createdFile = expectMsgType[ActorRef]
      new File(filename) should be ('exists)      
    }
  }  
}