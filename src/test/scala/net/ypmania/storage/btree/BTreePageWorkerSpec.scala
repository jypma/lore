package net.ypmania.storage.btree

import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.concurrent.Eventually
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.actor.ActorSystem
import scala.util.Random
import akka.testkit.TestProbe
import akka.testkit.TestActorRef
import akka.actor.Props
import net.ypmania.storage.paged.PageIdx
import net.ypmania.lore.ID
import net.ypmania.storage.paged.PagedStorage
import net.ypmania.storage.atomic.AtomicActor

class BTreePageWorkerSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                       with WordSpecLike with Matchers with Eventually {
  class Fixture {
    implicit val settings = BTree.Settings(order = 2)
    val pagedStorage = TestProbe()
    val tree = system.actorOf(Props(new BTree(pagedStorage.ref, PageIdx(0))))
    
    val r = pagedStorage.expectMsgType[PagedStorage.Read[BTreePage]]
    pagedStorage reply PagedStorage.ReadCompleted(BTreePage.empty)
  }
  
  "A B+Tree" should {
    "be empty on creation" in new Fixture {
      val id = ID.forBranch
      tree ! BTree.Get(id)
      expectMsgType[BTree.NotFound]
    }
    
    "remember a single entry" in new Fixture {
      val id = ID.forBranch
      val page = PageIdx(123)
      tree ! BTree.Put(id, page)
      expectMsgType[BTree.PutCompleted]
      
      val written = pagedStorage.expectMsgType[PagedStorage.Write]
      val content = written.pages(PageIdx(0)).content.asInstanceOf[BTreePage]
      content.get(id) should be (Some(page))
      
      tree ! BTree.Get(id)
      expectMsg(BTree.Found(page, None))
    }
    
    "split the root when 4 entries are added" in new Fixture {
      for (i <- 1 to 3) {
        tree ! BTree.Put(ID(0,i), PageIdx(123))
        expectMsgType[BTree.PutCompleted]

        val written = pagedStorage.expectMsgType[PagedStorage.Write]
        val page0 = written.pages(PageIdx(0))
        val page = page0.content.asInstanceOf[BTreePage]
        page.size should be (i)
        
        pagedStorage reply PagedStorage.WriteCompleted
      }
      tree ! BTree.Put(ID(0,4), PageIdx(123))
      
      pagedStorage.expectMsg(PagedStorage.ReservePage)
      pagedStorage.reply(PagedStorage.PageReserved(PageIdx(1)))

      val writes = pagedStorage.receiveWhile() { 
        case write @ AtomicActor.Atomic(PagedStorage.Write(_), _, _) => Some(write)
        case PagedStorage.ReservePage => None // root reserving page
        case PagedStorage.Write(pages) if pages.contains(PageIdx(0)) => None // adding node "4"
        case other => println(other); None
      }
      
      writes.filter(_.isDefined).map(_.get) should have size(2)
      
      /*
      // write 1 should contain new left and write node, which was the old root
      val write1 = pagedStorage.expectMsgType[AtomicActor.Atomic[PagedStorage.Write]]
      write1.msg.pages should contain key(PageIdx(0))
      write1.msg.pages should contain key(PageIdx(1))
      
      // now, the root has gotten the split message and creates a new root
      pagedStorage.expectMsg(PagedStorage.ReservePage)
      pagedStorage.reply(PagedStorage.PageReserved(PageIdx(2)))
      
      val msgs = pagedStorage.receiveN(2)
      println(msgs)
      
      //newRootWrite.msg.pages should contain key(PageIdx(2))      
      //newRootWrite.atom should be (write1.atom)
      */
    }
  }
}