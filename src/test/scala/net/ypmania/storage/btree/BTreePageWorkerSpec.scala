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
        tree ! BTree.Put(ID.forBranch, PageIdx(123))
        expectMsgType[BTree.PutCompleted]
        val written = pagedStorage.expectMsgType[PagedStorage.Write]
        pagedStorage reply PagedStorage.WriteCompleted
      }
      tree ! BTree.Put(ID.forBranch, PageIdx(123))
      
    }
  }
}