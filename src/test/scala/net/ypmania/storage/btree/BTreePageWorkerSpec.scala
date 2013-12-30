package net.ypmania.storage.btree

import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.concurrent.Eventually
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.actor.ActorSystem
import scala.util.Random

class BTreePageWorkerSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                       with WordSpecLike with Matchers with Eventually {
  trait Fixture {
    val filename = "/tmp/PagedFileSpec" + Random.nextInt
    
  }
  
  "A B+Tree" should {
    
  }
}