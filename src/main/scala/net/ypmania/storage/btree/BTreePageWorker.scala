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

class BTreePageWorker private[btree] (pagedStorage: ActorRef, pageIdx: PageIdx, isRoot: Boolean)
                     (implicit val settings: BTree.Settings) 
                     extends Actor with Stash with ActorLogging {
  import context.dispatcher
  
  import BTree._
  import BTreePage._
  import BTreePageWorker._
  
  log.debug(s"Starting worker for page ${pageIdx}")
  pagedStorage ! PagedStorage.Read[BTreePage](pageIdx)
  
  def receive = {
    case PagedStorage.ReadCompleted(page: BTreePage) =>
      log.debug(s"Page ${pageIdx} received as ${page}")
      unstashAll()
      context become active(page)
    case _ => 
      stash()
  }
  
  // Only during split do internal nodes grow
  def active(page: BTreePage): Receive = {
    case msg @ Put(key, value) =>
      log.debug(s"Received Put while at size ${page.size} of ${settings.entriesPerPage}")
      if (page.full) {
        log.debug(s"Splitting because of ${msg}")
        stash()
        if (isRoot) {
          pagedStorage ! PagedStorage.ReservePages(2)
          log.debug(s"Awaiting death of ${context.children}")
          for (child <- context.children) {
            context.watch(child)
            child ! PoisonPill
          }
          context become reservingForRootSplit(page)
        } else {
          pagedStorage ! PagedStorage.ReservePage
          context become reservingForSplit(page)          
        }
      } else page match {
        case internal:InternalBTreePage =>
          val destination = internal.lookup(key)
          log.debug(s"Forwarding ${msg} to child $destination")
          childForPage(destination) forward msg
        case leaf:LeafBTreePage =>
          val updated = page + (key -> value)
          log.debug(s"Writing ${updated}, replying to ${sender}")
          pagedStorage ! PagedStorage.Write(pageIdx -> updated)
          sender ! PutCompleted
          context become active(updated)          
      } 
      
    case msg @ Get(key) =>
      page match {
        case internal:InternalBTreePage =>
          log.debug(s"Forwarding $msg to child")
          val childPageIdx = internal.lookup(key)
          childForPage(childPageIdx) forward msg 
        
        case leaf:LeafBTreePage =>
          val reply = leaf.get(key) match {
            case Some(value) => Found(value)
            case None => NotFound
          }
          sender ! reply
      }
      
    case ApplySplit(info, atom) =>
      val updated = applySplit(page, info, atom)
      context become active(updated)
      
  }
  
  private def applySplit(page: BTreePage, info: SplitResult, atom: Atom) = {
    page match {
      case internal:InternalBTreePage if !internal.full =>
        val updated = internal.splice(info)
        pagedStorage ! Atomic(PagedStorage.Write(pageIdx -> updated), atom = atom, otherSenders = Set(sender))
        sender ! SplitApplied
        updated
    }
  }
  
  def reservingForSplit(page: BTreePage): Receive = {
    case PagedStorage.PageReserved(rightPageIdx) =>
      val (left, right, splitResult) = page.split(pageIdx, rightPageIdx)
      log.debug(s"Got reservation for new right page $rightPageIdx, splitting into $splitResult")
      val atom = Atom()
      pagedStorage ! Atomic(
          PagedStorage.Write(pageIdx -> left) + (rightPageIdx -> right),
          otherSenders = Set(context.parent),
          atom = atom)
      context.parent ! ApplySplit(splitResult, atom)
      context become awaitingSplitCompletion(left, context.parent)
      
    case other =>
      log.debug(s"reserving: stashing")
      stash()
  }
  
  def reservingForRootSplit(page: BTreePage): Receive = {
    var awaitingChildren = context.children.toSet
    var reservedPages:Option[(PageIdx, PageIdx)] = None
    
    def maybeDone() {
      if (awaitingChildren.isEmpty && reservedPages.isDefined) {
        log.debug("All children have died, and received reserved pages. Splitting.")
        val (leftPageIdx, rightPageIdx) = reservedPages.get
        unstashAll()
        val (left, right, splitResult) = page.split(leftPageIdx, rightPageIdx)
        val newRoot = splitResult.asRoot
        implicit val timeout = Timeout(10.seconds)
        pagedStorage ? (PagedStorage.Write(pageIdx -> newRoot) + 
                                          (leftPageIdx -> left) +
                                          (rightPageIdx -> right)) map {
          case PagedStorage.WriteCompleted => SplitApplied 
        } pipeTo self
        context become awaitingSplitCompletion(newRoot, self) 
      }
    }
    
    return {
      case Terminated(child) =>
        awaitingChildren -= child
        maybeDone()
      
      case PagedStorage.PagesReserved(leftPageIdx :: rightPageIdx :: Nil) =>
        reservedPages = Some(leftPageIdx, rightPageIdx)
        maybeDone()
        
      case ApplySplit(info, atom) =>
        val updated = applySplit(page, info, atom)
        context become reservingForRootSplit(updated)
        
      case _ =>
        log.debug("reservingForRoot: stashing")
        stash()
    }
  }
  
  def awaitingSplitCompletion(newPage:BTreePage, redeliverTarget: ActorRef): Receive = {
    case SplitApplied =>
      // From now on, it is safe to send pre-split messages back to the parent for re-routing.
      // Additionally, from now on, we won't get any wrongly routed messages anymore.
      unstashAll()
      self ! RedeliverForSplitCompleted
      context become redeliveringForSplit(newPage, redeliverTarget)
      
    case _ =>
      log.debug(s"awaiting: stashing")
      stash()
  }
  
  def redeliveringForSplit(newPage: BTreePage, target: ActorRef): Receive = {
    case RedeliverForSplitCompleted =>
      log.debug("Redeliveries done, completing split")
      context become active(newPage)
      
    case msg =>
      log.debug(s"Redelivering $msg to ${context.parent}")
      target forward msg
  }
  
  def childForPage(page: PageIdx) = {
    val name = page.toInt.toString
    context.child(name) getOrElse context.actorOf(Props(new BTreePageWorker(pagedStorage, page, false)), name)
  }
}

object BTreePageWorker {
  private[btree] case class ApplySplit(info: SplitResult, atom: Atom)
  private[btree] case object SplitApplied
  
  private object RedeliverForSplitCompleted
}
