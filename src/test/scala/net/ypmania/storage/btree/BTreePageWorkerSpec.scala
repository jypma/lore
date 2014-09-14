package net.ypmania.storage.btree

import akka.testkit.TestKit
import akka.testkit.ImplicitSender
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
import akka.actor.Actor
import akka.actor.ActorRef
import akka.dispatch.Envelope
import scala.collection.immutable.TreeMap
import net.ypmania.test.ParentingTestProbe

class BTreePageWorkerSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                          with WordSpecLike with Matchers {
  root =>
  
  trait BaseFixture {
    implicit val settings = BTree.Settings(order = 2)
    
    def initialContent = BTreePage.empty
    def childWorkerPages = Set.empty[PageIdx]
    
    def isRoot = false
    val pagedStorage = TestProbe()
    val childWorkers = childWorkerPages.map(_ -> TestProbe()).toMap
    
    val workerParent = ParentingTestProbe(Props(new BTreePageWorker(pagedStorage.ref, PageIdx(0), isRoot) {
        override def childForPage(page: PageIdx) = childWorkers(page).ref
    }))
    val worker = workerParent.child

    val r = pagedStorage.expectMsgType[PagedStorage.Read[BTreePage]]
    pagedStorage reply PagedStorage.ReadCompleted(initialContent)
  }
  
  "An empty B+Tree worker" should {
    trait Fixture extends BaseFixture
    
    "return no content when getting nodes" in new Fixture {
      val id = BaseID(1,1,1)
      worker ! BTree.Get(id)
      expectMsg(BTree.NotFound)
    }
    
    "remember a single entry" in new Fixture {
      val id = BaseID(1,1,1)
      val page = PageIdx(123)
      worker ! BTree.Put(id, page)
      expectMsg(BTree.PutCompleted)
      
      val written = pagedStorage.expectMsgType[PagedStorage.Write]
      val content = written.pages(PageIdx(0)).content.asInstanceOf[LeafBTreePage]
      content.get(id) should be (Some(page))
      
      worker ! BTree.Get(id)
      expectMsg(BTree.Found(page))
    }
    
    "split when the 4th entry is added as rightmost key" in new Fixture {
      for (i <- 1 to 3) {
        worker ! BTree.Put(BaseID(1,1,i), PageIdx(123))
        expectMsg(BTree.PutCompleted)

        val written = pagedStorage.expectMsgType[PagedStorage.Write]
        val page0 = written.pages(PageIdx(0))
        val page = page0.content.asInstanceOf[BTreePage]
        page.size should be (i)
        
        pagedStorage reply PagedStorage.WriteCompleted
      }
      val putMsg4 = BTree.Put(BaseID(1,1,4), PageIdx(123))
      worker ! putMsg4
      
      pagedStorage.expectMsg(PagedStorage.ReservePage)
      pagedStorage.reply(PagedStorage.PageReserved(PageIdx(1)))

      val split = workerParent.expectMsgType[BTreePageWorker.ApplySplit]
      split.info.leftPageIdx should be (PageIdx(0))
      split.info.rightPageIdx should be (PageIdx(1))
      split.info.key should be (BaseID(1,1,2))
      
      workerParent reply BTreePageWorker.SplitApplied
      workerParent.expectMsg(putMsg4) 
      
      val write = pagedStorage.expectMsgType[AtomicActor.Atomic[PagedStorage.Write]]
      write.atom should be (split.atom)
      write.otherSenders should be (Set(workerParent.actor))
      val updatedLeft = write.msg.pages(PageIdx(0)).content.asInstanceOf[LeafBTreePage]
      updatedLeft.pointers should be (Map(BaseID(1,1,1) -> PageIdx(123)))
      val newRight = write.msg.pages(PageIdx(1)).content.asInstanceOf[LeafBTreePage]
      newRight.pointers should be (Map(BaseID(1,1,2) -> PageIdx(123), BaseID(1,1,3) -> PageIdx(123)))
    }
    
  }
  
  "A B+Tree worker initialized as leaf node with 3 childs" should {
    trait Fixture extends BaseFixture {
      override def initialContent = LeafBTreePage( 
          TreeMap(BaseID(1,1,2) -> PageIdx(123), BaseID(1,1,3) -> PageIdx(123), BaseID(1,1,4) -> PageIdx(123)), 
          None)
      val childForPage1 = TestProbe()
    }
    
    "split on the next put message as leftmost key" in new Fixture {
      val putMsg = BTree.Put(BaseID(1,1,1), PageIdx(123))
      worker ! putMsg
      
      pagedStorage.expectMsg(PagedStorage.ReservePage) 
      pagedStorage.reply(PagedStorage.PageReserved(PageIdx(1)))

      val split = workerParent.expectMsgType[BTreePageWorker.ApplySplit]
      split.info.leftPageIdx should be (PageIdx(0))
      split.info.rightPageIdx should be (PageIdx(1))
      split.info.key should be (BaseID(1,1,3))
      workerParent reply BTreePageWorker.SplitApplied
      
      val write = pagedStorage.expectMsgType[AtomicActor.Atomic[PagedStorage.Write]]
      write.atom should be (split.atom)
      write.otherSenders should be (Set(workerParent.actor))
      val updatedLeft = write.msg.pages(PageIdx(0)).content.asInstanceOf[LeafBTreePage]
      updatedLeft.pointers should be (Map(BaseID(1,1,2) -> PageIdx(123)))
      val newRight = write.msg.pages(PageIdx(1)).content.asInstanceOf[LeafBTreePage]
      newRight.pointers should be (Map(BaseID(1,1,3) -> PageIdx(123), BaseID(1,1,4) -> PageIdx(123)))
      
      // The original put message is sent back to the parent for redelivery (it might need to have gone to the new node)
      workerParent.expectMsg(putMsg)
    }
    
    "forward stashed messages designated for the new child node during a split" in new Fixture {
      worker ! BTree.Put(BaseID(1,1,1), PageIdx(123))
      pagedStorage.expectMsg(PagedStorage.ReservePage) 

      // Intermediate put comes in that should go to the new (right) node
      worker ! BTree.Put(BaseID(1,1,5), PageIdx(123))
      
      // Intermediate put comes in that should go to the old (left) node
      worker ! BTree.Put(BaseID(1,1,0), PageIdx(123))
      
      pagedStorage.reply(PagedStorage.PageReserved(PageIdx(1)))
      val split = workerParent.expectMsgType[BTreePageWorker.ApplySplit]
      split.info.leftPageIdx should be (PageIdx(0))
      split.info.rightPageIdx should be (PageIdx(1))
      split.info.key should be (BaseID(1,1,3))
      workerParent reply BTreePageWorker.SplitApplied
      
      // The original put, and all intermediaries, are forward to the parent for redelivery
      workerParent.expectMsg(BTree.Put(BaseID(1,1,1), PageIdx(123)))
      workerParent.expectMsg(BTree.Put(BaseID(1,1,5), PageIdx(123)))
      workerParent.expectMsg(BTree.Put(BaseID(1,1,0), PageIdx(123)))
    }
  }
  
  "A B+Tree worker initialized as internal node with 3 childs" should {
    trait Fixture extends BaseFixture {
      override def initialContent = InternalBTreePage(TreeMap(
          BaseID(1,1,2) -> PageIdx(2), BaseID(1,1,3) -> PageIdx(3), BaseID(1,1,4) -> PageIdx(4)),
          PageIdx(5))
      override def childWorkerPages = Set(PageIdx(2))
    }
    
    "split on the next put message, then forward it to the parent" in new Fixture {
      val putMsg1 = BTree.Put(BaseID(1,1,1), PageIdx(123))
      worker ! putMsg1
      
      pagedStorage.expectMsg(PagedStorage.ReservePage) 
      pagedStorage.reply(PagedStorage.PageReserved(PageIdx(1)))

      val split = workerParent.expectMsgType[BTreePageWorker.ApplySplit]
      split.info.leftPageIdx should be (PageIdx(0))
      split.info.rightPageIdx should be (PageIdx(1))
      split.info.key should be (BaseID(1,1,3))
      workerParent reply BTreePageWorker.SplitApplied
      
      val write = pagedStorage.expectMsgType[AtomicActor.Atomic[PagedStorage.Write]]
      write.atom should be (split.atom)
      write.otherSenders should be (Set(workerParent.actor))
      val updatedLeft = write.msg.pages(PageIdx(0)).content.asInstanceOf[InternalBTreePage]
      updatedLeft.pointers should be (Map(BaseID(1,1,2) -> PageIdx(2)))
      val newRight = write.msg.pages(PageIdx(1)).content.asInstanceOf[InternalBTreePage]
      newRight.pointers should be (Map(BaseID(1,1,4) -> PageIdx(4)))
      
      workerParent.expectMsg(putMsg1)
    }
  }
  
  "A B+Tree worker initialized as internal node with 2 childs" should {
    trait Fixture extends BaseFixture {
      override def initialContent = InternalBTreePage(TreeMap(
          BaseID(1,1,10) -> PageIdx(1), BaseID(1,1,30) -> PageIdx(3)),
          PageIdx(4))
      override def childWorkerPages = Set(PageIdx(1), PageIdx(2), PageIdx(3), PageIdx(4))
    }
    
    "when receiving a Split, correctly update itself" in new Fixture {
      object StashMessage
      val stashSender = TestProbe()
      val atom = AtomicActor.Atom()
      worker ! BTreePageWorker.ApplySplit(BTreePage.SplitResult(PageIdx(1), BaseID(1,1,5), PageIdx(2)), atom)
      
      val write = pagedStorage.expectMsgType[AtomicActor.Atomic[PagedStorage.Write]]
      write.atom should be (atom)
      write.otherSenders should be (Set(self))
      val updated = write.msg.pages(PageIdx(0)).content.asInstanceOf[BTreePage]
      updated.pointers should be (Map(BaseID(1,1,5) -> PageIdx(1), BaseID(1,1,10) -> PageIdx(2), BaseID(1,1,30) -> PageIdx(3)))
      
      expectMsg(BTreePageWorker.SplitApplied)
    }
  }
  
  "A B+Tree worker initialized as internal root node with 3 childs" should {
    trait Fixture extends BaseFixture {
      override def isRoot = true
      override def initialContent = InternalBTreePage(TreeMap(
          BaseID(1,1,2) -> PageIdx(2), BaseID(1,1,3) -> PageIdx(3), BaseID(1,1,4) -> PageIdx(4)),
          PageIdx(5))
      override def childWorkerPages = Set(PageIdx(3), PageIdx(7))
    }

    "when receiving another put, kill all childs, split off a new left and right node, and remain root" in new Fixture {
      val putMsg1 = BTree.Put(BaseID(1,1,1), PageIdx(123))
      worker ! putMsg1
      
      pagedStorage.expectMsg(PagedStorage.ReservePages(2)) 
      pagedStorage.reply(PagedStorage.PagesReserved(PageIdx(7) :: PageIdx(8) :: Nil))

      val write = pagedStorage.expectMsgType[PagedStorage.Write]
      val newRoot = write.pages(PageIdx(0)).content.asInstanceOf[InternalBTreePage]
      newRoot should be (InternalBTreePage(TreeMap(BaseID(1,1,3) -> PageIdx(7)), PageIdx(8)))
      val newLeft = write.pages(PageIdx(7)).content.asInstanceOf[InternalBTreePage]
      newLeft.pointers should be (Map(BaseID(1,1,2) -> PageIdx(2)))
      val newRight = write.pages(PageIdx(8)).content.asInstanceOf[InternalBTreePage]
      newRight.pointers should be (Map(BaseID(1,1,4) -> PageIdx(4)))
      
      pagedStorage.reply(PagedStorage.WriteCompleted)
      
      // The original Put for BaseId(1,1,1) will now be forwarded to PageIdx(7), since that's what the new root says
      childWorkers(PageIdx(7)).expectMsg(putMsg1)
    }
  }
}