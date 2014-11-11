package net.ypmania.storage.btree

import akka.actor.ActorLogging
import akka.actor.Actor
import net.ypmania.storage.paged.PageIdx
import akka.actor.ActorRef
import net.ypmania.lore.ID
import net.ypmania.storage.paged.PagedStorage
import akka.actor.Props
import akka.actor.Stash
import scala.collection.immutable.TreeMap
import akka.dispatch.Envelope
import net.ypmania.storage.atomic.AtomicActor.Atom
import net.ypmania.storage.atomic.AtomicActor.Atomic
import net.ypmania.storage.btree.BTreePage.SplitResult
import akka.actor.PoisonPill
import akka.actor.Terminated
import akka.pattern.ask
import akka.pattern.pipe
import akka.util.Timeout
import scala.concurrent.duration._
import com.typesafe.scalalogging.StrictLogging
import akka.actor.Status.Failure

class BTreePageWorker private[btree] (pagedStorage: ActorRef, pageIdx: PageIdx, isRoot: Boolean)
                     (implicit val settings: BTree.Settings) 
                     extends Actor with Stash with ActorLogging {
  import context.dispatcher
  
  import BTree._
  import BTreePage._
  import BTreePageWorker._
  
  log.debug("Starting worker for page {}", pageIdx)
  pagedStorage ! PagedStorage.Read[BTreePage](pageIdx)
  
  private val stopOnFailure: Receive = {
    case Failure(x) =>
      log.warning("Received failure, terminating.", x)
      context.stop(self)
  }
  
  def receive = {
    case PagedStorage.ReadCompleted(page: BTreePage) =>
      log.debug("Page {} received as {}", pageIdx, page)
      unstashAll()
      context become active(page)
    case _ => 
      stash()
  }
  
  // Only during split do internal nodes grow
  def active(page: BTreePage): Receive = stopOnFailure.orElse {
    case msg @ Put(key, value) =>
      log.debug("Received Put while at size {} of {}", page.size, settings.entriesPerPage)
      if (page.full) {
        log.debug("Splitting because of {}", msg)
        stash()
        if (isRoot) {
          pagedStorage ! PagedStorage.ReservePages(2)
          context become reservingForRootSplit(page)
        } else {
          pagedStorage ! PagedStorage.ReservePage
          context become reservingForSplit(page)          
        }
      } else page match {
        case internal:InternalBTreePage =>
          val destination = internal.lookup(key)
          log.debug("Forwarding {} to child {}", msg, destination)
          context.parent forward ToChild(destination, msg)
        case leaf:LeafBTreePage =>
          val updated = page + (key -> value)
          log.debug("Writing {}, replying to {}", updated, sender)
          implicit val timeout = Timeout(10.minutes)
          pagedStorage ? PagedStorage.Write(pageIdx -> updated) map {
            case PagedStorage.WriteCompleted => PutCompleted
          } pipeTo sender
          context become active(updated)          
      } 
      
    case msg @ Get(key) =>
      page match {
        case internal:InternalBTreePage =>
          log.debug("Forwarding {} to child", msg)
          val childPageIdx = internal.lookup(key)
          context.parent forward ToChild(childPageIdx, msg)
        
        case leaf:LeafBTreePage =>
          val reply = leaf.get(key) match {
            case Some(value) => Found(value)
            case None => NotFound
          }
          sender ! reply
      }
      
    case ApplySplit(info, atom, otherAtoms) =>
      val updated = applySplit(page, info, atom, otherAtoms)
      context become active(updated)
      
    case PagedStorage.WriteCompleted =>
      log.debug("Ignoring a completed write.")
  }
  
  private def applySplit(page: BTreePage, info: SplitResult, atom: Atom, otherAtoms: Set[Atom]) = {
    log.debug("Applying split {} while on page {} with atom {} from {}", info, page, atom, sender)
    page match {
      case internal:InternalBTreePage if !internal.full =>
        val updated = internal.splice(info)
        pagedStorage ! Atomic(PagedStorage.Write(pageIdx -> updated), atom, otherAtoms)
        updated
    }
  }
  
  def reservingForSplit(page: BTreePage): Receive = stopOnFailure.orElse {
    case PagedStorage.PageReserved(rightPageIdx) =>
      val (left, right, splitResult) = page.split(pageIdx, rightPageIdx)
      log.debug("Got reservation for new right page {}, splitting into {}", rightPageIdx, splitResult)
      val atom1, atom2 = Atom()
      pagedStorage ! Atomic(
          PagedStorage.Write(pageIdx -> left) + (rightPageIdx -> right),
          otherAtoms = Set(atom2),
          atom = atom1)
      log.debug("Sending ApplySplit to {}", context.parent)
      context.parent ! ApplySplit(splitResult, atom2, Set(atom1))
      context become awaitingSplitCompletion(left, context.parent)
      
    case other =>
      log.debug("reserving: stashing {}", other)
      stash()
  }
  
  def reservingForRootSplit(page: BTreePage): Receive = stopOnFailure.orElse {
    case PagedStorage.PagesReserved(leftPageIdx :: rightPageIdx :: Nil) =>
      val (left, right, splitResult) = page.split(leftPageIdx, rightPageIdx)
      val newRoot = splitResult.asRoot
      pagedStorage ! (PagedStorage.Write(pageIdx -> newRoot) + 
                                        (leftPageIdx -> left) +
                                        (rightPageIdx -> right))
      context become awaitingSplitCompletion(newRoot, self) 
      
    case ApplySplit(info, atom, otherAtoms) =>
      val updated = applySplit(page, info, atom, otherAtoms)
      context become reservingForRootSplit(updated)
      
    case PagedStorage.WriteCompleted =>
      log.debug("Ignoring a completed write.")
      
    case other =>
      log.debug("reservingForRoot: stashing {}", other)
      stash()
  }
  
  def awaitingSplitCompletion(newPage:BTreePage, redeliverTarget: ActorRef): Receive = stopOnFailure.orElse {
    case PagedStorage.WriteCompleted =>
      // From now on, it is safe to send pre-split messages back to the parent for re-routing.
      // Additionally, from now on, we won't get any wrongly routed messages anymore.
      unstashAll()
      self ! RedeliverForSplitCompleted
      context become redeliveringForSplit(newPage, redeliverTarget)
      
    case other =>
      log.debug("awaiting: stashing {}", other)
      stash()
  }
  
  def redeliveringForSplit(newPage: BTreePage, target: ActorRef): Receive = stopOnFailure.orElse {
    case RedeliverForSplitCompleted =>
      log.debug("Redeliveries done, completing split")
      context become active(newPage)
      
    case msg =>
      log.debug("Redelivering {} to {}", msg, context.parent)
      target forward msg
  }
}

object BTreePageWorker {
  //private[btree] case object SplitApplied
  
  private object RedeliverForSplitCompleted
}
