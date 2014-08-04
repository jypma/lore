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
import net.ypmania.lore.BaseID
import java.io.File
import net.ypmania.storage.paged.DataHeader
import net.ypmania.storage.atomic.AtomicActor
import scala.concurrent.duration._

class StorageIntegrationSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                       with WordSpecLike with Matchers with Eventually {
  class Fixture {
    implicit val settings = BTree.Settings(order = 2)
    val randomInt = Random.nextInt
    val timeout = 2.seconds
    val filename = "/tmp/PagedFileSpec" + randomInt 
    val pagedStorage = system.actorOf(Props(new PagedStorage(filename)), "storage" + randomInt)
    val atomicStorage = system.actorOf(Props(new AtomicActor(pagedStorage, timeout)), "atomic" + randomInt)
    val tree = system.actorOf(Props(new BTree(atomicStorage, PageIdx(0))), "tree" + randomInt)
  }

  "A B-Tree in structured storage" should {
    
    "persist when re-opening the same file" in new Fixture {
      for (i <- 1 to 4) {
        tree ! BTree.Put(BaseID(1,1,i), PageIdx(i))
        expectMsgType[BTree.PutCompleted]
      }
      
      watch(pagedStorage)
      atomicStorage ! PagedStorage.Shutdown
      expectTerminated(pagedStorage, 2.seconds)
      
      val pagedStorage2 = system.actorOf(Props(new PagedStorage(filename)), "re_storage")
      val atomicStorage2 = system.actorOf(Props(new AtomicActor(pagedStorage2, timeout)), "re_atomic")
      val tree2 = system.actorOf(Props(new BTree(atomicStorage2, PageIdx(0))), "re_tree")
      
      tree2 ! BTree.Get(BaseID(1,1,1))
      expectMsg(BTree.Found(PageIdx(1), None))
      tree2 ! BTree.Get(BaseID(1,1,2))
      expectMsg(BTree.Found(PageIdx(2), None))
      tree2 ! BTree.Get(BaseID(1,1,3))
      expectMsg(BTree.Found(PageIdx(3), None))
      tree2 ! BTree.Get(BaseID(1,1,4))
      expectMsg(BTree.Found(PageIdx(4), None))
    }
  }
}