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
import net.ypmania.storage.btree.BTree
import net.ypmania.storage.btree.BTreePageWorker

class StorageIntegrationSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                       with WordSpecLike with Matchers with Eventually {
  trait BaseFixture {
    implicit def settings = BTree.Settings(order = 2)
    val randomInt = Random.nextInt
    def timeout = 10.minutes
    val filename = "/tmp/PagedFileSpec" + randomInt 
    val pagedStorage = system.actorOf(Props(new PagedStorage(filename)), "storage" + randomInt)
    val atomicStorage = system.actorOf(Props(new AtomicActor(pagedStorage, timeout)), "atomic" + randomInt)
    val tree = system.actorOf(BTree.props(atomicStorage, PageIdx(0)), "tree" + randomInt)
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
      watch(pagedStorage)
      atomicStorage ! PagedStorage.Shutdown
      expectTerminated(pagedStorage, 2.seconds)
      
      val pagedStorage2 = system.actorOf(Props(new PagedStorage(filename)), "re_storage")
      val atomicStorage2 = system.actorOf(Props(new AtomicActor(pagedStorage2, timeout)), "re_atomic")
      val tree2 = system.actorOf(BTree.props(atomicStorage2, PageIdx(0)), "re_tree")
      
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
  
  "A BTree with lots of nodes added in parallel" should {
    trait Fixture extends BaseFixture {
      override def settings = BTree.Settings(order = 2500)
      val count = 10000
      val entries = (0 until count).map(i => (BaseID(1,1,Math.abs(Random.nextInt())), PageIdx(i))).toMap
      
      val start = System.currentTimeMillis()
      for ((id, idx) <- entries) {
        tree ! BTree.Put(id, idx)
      }
      within(timeout) {
        for ((id, idx) <- entries) {
          expectMsg(BTree.PutCompleted)        
        }        
      }
      val ms = System.currentTimeMillis() - start
      println(s"Took ${ms}ms to write ${count} to ${filename}, ${ms.toFloat / count}ms per entry")
    }    
    
    "remember them while still in memory" in new Fixture {
      for ((id, idx) <- entries) {
        tree ! BTree.Get(id)
        expectMsg(BTree.Found(idx))        
      }
    }
  } 
}