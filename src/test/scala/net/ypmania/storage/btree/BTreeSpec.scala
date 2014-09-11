package net.ypmania.storage.btree

import org.scalatest.Matchers
import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.WordSpecLike
import akka.actor.ActorSystem
import akka.testkit.TestProbe
import net.ypmania.storage.paged.PageIdx
import akka.actor.Props
import net.ypmania.lore.BaseID
import net.ypmania.storage.atomic.AtomicActor
import akka.dispatch.Envelope
import scala.concurrent.duration._
import net.ypmania.storage.paged.PagedStorage

class BTreeSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                   with WordSpecLike with Matchers {
  trait Fixture {
    def workerPages = Set(PageIdx(0))
    
    implicit val settings = BTree.Settings(order = 2)
    val worker = TestProbe()
    val storage = TestProbe()
    val workers = workerPages.map(_ -> TestProbe()).toMap
    val btree = system.actorOf(Props(new BTree(storage.ref, PageIdx(0)) {
      override def workerActorOf(page: PageIdx) = workers(page).ref
    }))
  }
  
  "An idle BTree actor" should {
    "forward messages to its root" in new Fixture {
      val putMsg = BTree.Put(BaseID(1,1,1), PageIdx(123))
      btree ! putMsg
      workers(PageIdx(0)).expectMsg(putMsg)
      workers(PageIdx(0)).lastSender should be (self)
    }
    
    "when receiving Split, create a new root, and deliver stashed messages for the rightmost node" in new Fixture {
      override def workerPages = Set(PageIdx(0), PageIdx(1), PageIdx(2))
      
      object StashedMsg
      val stashSender = TestProbe()
      val atom = AtomicActor.Atom()
      
      val oldRootActor = workers(PageIdx(0)).ref
      watch(oldRootActor)
      
      val putWhileSplitting = BTree.Put(BaseID(1,1,1), PageIdx(123))
      btree ! BTree.Split(BTreePage.SplitResult(PageIdx(0), BaseID(1,1,10), PageIdx(1)), Vector(Envelope(StashedMsg, stashSender.ref, system)), atom)
      btree ! putWhileSplitting
      
      expectTerminated(oldRootActor, 1.second)
      
      storage.expectMsg(PagedStorage.ReservePage)
      storage.reply(PagedStorage.PageReserved(PageIdx(2)))
      
      val write = storage.expectMsgType[AtomicActor.Atomic[PagedStorage.Write]]
      write.atom should be (atom)
      write.otherSenders should contain (oldRootActor)
      val page = write.msg.pages(PageIdx(2)).content.asInstanceOf[InternalBTreePage]
      page.pointers should be (Map(BaseID(1,1,10) -> PageIdx(0)))
      page.next should be (PageIdx(1))
      
      // stashed message for the new rightmost node will be re-sent to the new root, which is managing the rightmost node
      workers(PageIdx(2)).expectMsg(StashedMsg)
      workers(PageIdx(2)).lastSender should be (stashSender.ref)
      
      // messages received while splitting should be unstashed after split is complete, and arrive at the new root
      workers(PageIdx(2)).expectMsg(putWhileSplitting)
    }
  }
  
}