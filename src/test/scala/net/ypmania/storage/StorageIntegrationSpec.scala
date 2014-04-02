package net.ypmania.storage

import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.concurrent.Eventually
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.actor.ActorSystem
import scala.util.Random
import akka.actor.Props
import net.ypmania.storage.btree.BTree
import net.ypmania.storage.paged.PagedStorage
import net.ypmania.storage.paged.PageIdx
import net.ypmania.lore.ID
import java.io.File
import net.ypmania.storage.paged.DataHeader
import net.ypmania.storage.atomic.AtomicActor
import scala.concurrent.duration._

class StorageIntegrationSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                       with WordSpecLike with Matchers with Eventually {
  class Fixture {
    implicit val settings = BTree.Settings(order = 2)
    val timeout = 2.seconds
    val filename = "/tmp/PagedFileSpec" + Random.nextInt 
    val pagedStorage = system.actorOf(Props(new PagedStorage(filename)), "storage")
    val atomicStorage = system.actorOf(Props(new AtomicActor(pagedStorage, timeout)))
    val tree = system.actorOf(Props(new BTree(atomicStorage, PageIdx(0))), "tree")
  }

  "A B-Tree" should {
    "split into a new node when overflowing" in new Fixture {
      for (i <- 1 to 4) {
        tree ! BTree.Put(ID(0,i), PageIdx(i))
      }
      eventually {
        // 3 BTree nodes
        new File(filename) should have length(64*1024*3 + DataHeader.size)
      }
    }
  }
}