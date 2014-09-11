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
  trait BaseFixture {
    implicit val settings = BTree.Settings(order = 2)
    val randomInt = Random.nextInt
    val timeout = 2.seconds
    val filename = "/tmp/PagedFileSpec" + randomInt 
    val pagedStorage = system.actorOf(Props(new PagedStorage(filename)), "storage" + randomInt)
    val atomicStorage = system.actorOf(Props(new AtomicActor(pagedStorage, timeout)), "atomic" + randomInt)
    val tree = system.actorOf(Props(new BTree(atomicStorage, PageIdx(0))), "tree" + randomInt)
  }
  
  "A B-Tree initialized with 2 elements" should {
    trait Fixture extends BaseFixture {
      tree ! BTree.Put(BaseID(1,1,1), PageIdx(101))
      expectMsg(BTree.PutCompleted)
      tree ! BTree.Put(BaseID(1,1,2), PageIdx(102))
      expectMsg(BTree.PutCompleted)
    }
    
    "contain both elements" in new Fixture {
      tree ! BTree.Get(BaseID(1,1,1))
      expectMsg(BTree.Found(PageIdx(101)))
      tree ! BTree.Get(BaseID(1,1,2))
      expectMsg(BTree.Found(PageIdx(102)))      
    }
  }

  "A B-Tree initialized with 4 elements, resulting in 3 used pages" should {
    trait Fixture extends BaseFixture {
      tree ! BTree.Put(BaseID(1,1,1), PageIdx(101))
      expectMsg(BTree.PutCompleted)
      tree ! BTree.Put(BaseID(1,1,2), PageIdx(102))
      expectMsg(BTree.PutCompleted)
      tree ! BTree.Put(BaseID(1,1,3), PageIdx(103))
      expectMsg(BTree.PutCompleted)
      tree ! BTree.Put(BaseID(1,1,4), PageIdx(104))
      expectMsg(BTree.PutCompleted)     
    }
    
    "contain all 4 elements" in new Fixture {
      tree ! BTree.Get(BaseID(1,1,1))
      expectMsg(BTree.Found(PageIdx(101)))
      tree ! BTree.Get(BaseID(1,1,2))
      expectMsg(BTree.Found(PageIdx(102)))
      tree ! BTree.Get(BaseID(1,1,3))
      expectMsg(BTree.Found(PageIdx(103)))
      tree ! BTree.Get(BaseID(1,1,4))
      expectMsg(BTree.Found(PageIdx(104)))      
    }
    
    "persist when re-opening the same file" in new Fixture {
      // TODO have protocol for when tree changes root node, which is now at PageIdx(2) instead of PageIdx(0).
      
      watch(pagedStorage)
      atomicStorage ! PagedStorage.Shutdown
      expectTerminated(pagedStorage, 2.seconds)
      
      val pagedStorage2 = system.actorOf(Props(new PagedStorage(filename)), "re_storage")
      val atomicStorage2 = system.actorOf(Props(new AtomicActor(pagedStorage2, timeout)), "re_atomic")
      val tree2 = system.actorOf(Props(new BTree(atomicStorage2, PageIdx(0))), "re_tree")
      
      tree2 ! BTree.Get(BaseID(1,1,1))
      expectMsg(BTree.Found(PageIdx(101)))
      tree2 ! BTree.Get(BaseID(1,1,2))
      expectMsg(BTree.Found(PageIdx(102)))
      tree2 ! BTree.Get(BaseID(1,1,3))
      expectMsg(BTree.Found(PageIdx(103)))
      tree2 ! BTree.Get(BaseID(1,1,4))
      expectMsg(BTree.Found(PageIdx(104)))
    }
  }
}