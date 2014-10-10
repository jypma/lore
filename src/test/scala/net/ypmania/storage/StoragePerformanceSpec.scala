package net.ypmania.storage

import akka.testkit.TestKit
import net.ypmania.storage.paged.PagedStorage
import akka.testkit.ImplicitSender
import akka.actor.Props
import net.ypmania.storage.btree.BTree
import org.scalatest.concurrent.Eventually
import net.ypmania.storage.paged.PageIdx
import org.scalatest.Matchers
import net.ypmania.storage.atomic.AtomicActor
import scala.util.Random
import org.scalatest.WordSpecLike
import akka.actor.ActorSystem
import scala.concurrent.duration._
import net.ypmania.lore.BaseID

class StoragePerformanceSpec  extends TestKit(ActorSystem("Test")) with ImplicitSender 
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
  
  "A BTree with lots of nodes added in parallel" should {
    trait Fixture extends BaseFixture {
      override def settings = BTree.Settings(order = 2500)
      val count = 400000
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