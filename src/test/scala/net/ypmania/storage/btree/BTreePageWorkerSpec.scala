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

class BTreePageWorkerSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                          with WordSpecLike with Matchers {
  root =>
  
  trait BaseFixture {
    implicit val settings = BTree.Settings(order = 2)
    
    def initialContent = BTreePage.empty
    def childWorkerPages = Set.empty[PageIdx]
    
    val pagedStorage = TestProbe()
    val childWorkers = childWorkerPages.map(_ -> TestProbe()).toMap
    val workerParent = system.actorOf(Props(new Actor {
      val worker = context.actorOf(Props(new BTreePageWorker(pagedStorage.ref, PageIdx(0)) {
        override def childForPage(page: PageIdx) = childWorkers(page).ref
      }), "worker")
      root.self ! worker
      def receive = {
        case x if sender == worker => testActor forward x
        case x => worker forward x
      }
    }))
    
    val worker = expectMsgType[ActorRef]
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
    
    "split the root when the 4th entry is added as rightmost key" in new Fixture {
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

      val split = expectMsgType[BTree.Split]
      split.info.leftPageIdx should be (PageIdx(0))
      split.info.rightPageIdx should be (PageIdx(1))
      split.info.key should be (BaseID(1,1,2))
      // 4th message is to the right of the split key, so it should go to the new node
      split.stash should be (Vector(Envelope(putMsg4, self, system))) 
      
      val write = pagedStorage.expectMsgType[AtomicActor.Atomic[PagedStorage.Write]]
      write.atom should be (split.atom)
      write.otherSenders should be (Set(workerParent))
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
      worker ! BTree.Put(BaseID(1,1,1), PageIdx(123))
      
      pagedStorage.expectMsg(PagedStorage.ReservePage) 
      pagedStorage.reply(PagedStorage.PageReserved(PageIdx(1)))

      val split = expectMsgType[BTree.Split]
      split.info.leftPageIdx should be (PageIdx(0))
      split.info.rightPageIdx should be (PageIdx(1))
      split.info.key should be (BaseID(1,1,3))
      split.stash should be ('empty) 
      
      val write = pagedStorage.expectMsgType[AtomicActor.Atomic[PagedStorage.Write]]
      write.atom should be (split.atom)
      write.otherSenders should be (Set(workerParent))
      val updatedLeft = write.msg.pages(PageIdx(0)).content.asInstanceOf[LeafBTreePage]
      updatedLeft.pointers should be (Map(BaseID(1,1,2) -> PageIdx(123)))
      val newRight = write.msg.pages(PageIdx(1)).content.asInstanceOf[LeafBTreePage]
      newRight.pointers should be (Map(BaseID(1,1,3) -> PageIdx(123), BaseID(1,1,4) -> PageIdx(123)))
      
      val writeAfterUnstash = pagedStorage.expectMsgType[PagedStorage.Write].pages(PageIdx(0)).content.asInstanceOf[LeafBTreePage]
      writeAfterUnstash.pointers should be (Map(BaseID(1,1,1) -> PageIdx(123), BaseID(1,1,2) -> PageIdx(123)))
      
      expectMsg(BTree.PutCompleted)
    }
    
    "forward stashed messages designated for the new child node during a split" in new Fixture {
      worker ! BTree.Put(BaseID(1,1,1), PageIdx(123))
      pagedStorage.expectMsg(PagedStorage.ReservePage) 

      // Intermediate put comes in that should go to the new (right) node
      val putMsg5 = BTree.Put(BaseID(1,1,5), PageIdx(123))
      worker ! putMsg5
      
      // Intermediate put comes in that should go to the old (left) node
      worker ! BTree.Put(BaseID(1,1,0), PageIdx(123))
      
      pagedStorage.reply(PagedStorage.PageReserved(PageIdx(1)))
      val split = expectMsgType[BTree.Split]
      split.info.leftPageIdx should be (PageIdx(0))
      split.info.rightPageIdx should be (PageIdx(1))
      split.info.key should be (BaseID(1,1,3))
      split.stash should be (Vector(Envelope(putMsg5, self, system))) 
      
      expectMsg(BTree.PutCompleted)
      expectMsg(BTree.PutCompleted)
    }
  }
  
  "A B+Tree worker initialized as internal node with 3 childs" should {
    trait Fixture extends BaseFixture {
      override def initialContent = InternalBTreePage(TreeMap(
          BaseID(1,1,2) -> PageIdx(2), BaseID(1,1,3) -> PageIdx(3), BaseID(1,1,4) -> PageIdx(4)),
          PageIdx(5))
      override def childWorkerPages = Set(PageIdx(2))
    }
    
    "split on the next put message, then forward it to the new child" in new Fixture {
      val putMsg1 = BTree.Put(BaseID(1,1,1), PageIdx(123))
      worker ! putMsg1
      
      pagedStorage.expectMsg(PagedStorage.ReservePage) 
      pagedStorage.reply(PagedStorage.PageReserved(PageIdx(1)))

      val split = expectMsgType[BTree.Split]
      split.info.leftPageIdx should be (PageIdx(0))
      split.info.rightPageIdx should be (PageIdx(1))
      split.info.key should be (BaseID(1,1,3))
      split.stash should be ('empty) 
      
      val write = pagedStorage.expectMsgType[AtomicActor.Atomic[PagedStorage.Write]]
      write.atom should be (split.atom)
      write.otherSenders should be (Set(workerParent))
      val updatedLeft = write.msg.pages(PageIdx(0)).content.asInstanceOf[InternalBTreePage]
      updatedLeft.pointers should be (Map(BaseID(1,1,2) -> PageIdx(2)))
      val newRight = write.msg.pages(PageIdx(1)).content.asInstanceOf[InternalBTreePage]
      newRight.pointers should be (Map(BaseID(1,1,4) -> PageIdx(4)))
      
      childWorkers(PageIdx(2)).expectMsg(putMsg1)
    }
  }
  
  "A B+Tree worker initialized as internal node with 2 childs" should {
    trait Fixture extends BaseFixture {
      override def initialContent = InternalBTreePage(TreeMap(
          BaseID(1,1,10) -> PageIdx(1), BaseID(1,1,30) -> PageIdx(3)),
          PageIdx(4))
      override def childWorkerPages = Set(PageIdx(1), PageIdx(2), PageIdx(3), PageIdx(4))
    }
    
    "when receiving a Split, correctly update itself, start a new child, and forward msgs to it" in new Fixture {
      object StashMessage
      val stashSender = TestProbe()
      val atom = AtomicActor.Atom()
      worker ! BTree.Split(BTreePage.SplitResult(PageIdx(1), BaseID(1,1,5), PageIdx(2)), 
                           Vector(Envelope(StashMessage, stashSender.ref, system)), atom)
      
      val write = pagedStorage.expectMsgType[AtomicActor.Atomic[PagedStorage.Write]]
      write.atom should be (atom)
      write.otherSenders should be (Set(self))
      val updated = write.msg.pages(PageIdx(0)).content.asInstanceOf[BTreePage]
      updated.pointers should be (Map(BaseID(1,1,5) -> PageIdx(1), BaseID(1,1,10) -> PageIdx(2), BaseID(1,1,30) -> PageIdx(3)))
      
      childWorkers(PageIdx(2)).expectMsg(StashMessage)
      childWorkers(PageIdx(2)).lastSender should be (stashSender.ref)
    }
  }
}