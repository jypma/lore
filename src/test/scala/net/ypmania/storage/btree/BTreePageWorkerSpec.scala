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
import net.ypmania.lore.BaseID
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
      val id = BaseID(1,1,1)
      tree ! BTree.Get(id)
      expectMsg(BTree.NotFound)
    }
    
    "remember a single entry" in new Fixture {
      val id = BaseID(1,1,1)
      val page = PageIdx(123)
      tree ! BTree.Put(id, page)
      expectMsg(BTree.PutCompleted)
      
      val written = pagedStorage.expectMsgType[PagedStorage.Write]
      val content = written.pages(PageIdx(0)).content.asInstanceOf[BTreePage]
      content.get(id) should be (Some(page))
      
      tree ! BTree.Get(id)
      expectMsg(BTree.Found(page))
    }
    
    "split the root when 4 entries are added" in new Fixture {
      for (i <- 1 to 3) {
        tree ! BTree.Put(BaseID(1,1,i), PageIdx(123))
        expectMsg(BTree.PutCompleted)

        val written = pagedStorage.expectMsgType[PagedStorage.Write]
        val page0 = written.pages(PageIdx(0))
        val page = page0.content.asInstanceOf[BTreePage]
        page.size should be (i)
        
        pagedStorage reply PagedStorage.WriteCompleted
      }
      tree ! BTree.Put(BaseID(1,1,4), PageIdx(123))
      
      pagedStorage.expectMsg(PagedStorage.ReservePage) // worker reserving a new page for the new split-off node
      pagedStorage.reply(PagedStorage.PageReserved(PageIdx(1)))

      val NewRootPage = PageIdx(2)
      val writes = pagedStorage.receiveWhile() { 
        case write @ AtomicActor.Atomic(PagedStorage.Write(_), _, _) =>   // old root splitting in atomic write 
          Some(write.asInstanceOf[AtomicActor.Atomic[PagedStorage.Write]]) 
        case PagedStorage.ReservePage =>                                  // root reserving page 
          pagedStorage.reply(PagedStorage.PageReserved(NewRootPage)); 
          None 
        //case r@PagedStorage.Read(NewRootPage) =>                          // new root reading itself  
        //  pagedStorage.reply(r.emptyResult); None 
        case PagedStorage.Write(pages) if pages.contains(PageIdx(0)) =>   // finally adding node "4" 
          None 
      }.filter(_.isDefined).map(_.get)
      
      writes should have size(2)
      
      writes(0).atom should be (writes(1).atom)
      val combinedWrite = writes(0).msg ++ writes(1).msg
      combinedWrite.pages should contain key(PageIdx(0))
      combinedWrite.pages should contain key(PageIdx(1))
      combinedWrite.pages should contain key(PageIdx(2))
    }
  }
}