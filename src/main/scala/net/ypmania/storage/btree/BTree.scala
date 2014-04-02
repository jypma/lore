package net.ypmania.storage.btree

import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.ActorRef
import net.ypmania.storage.paged.PageIdx
import net.ypmania.lore.ID
import akka.dispatch.Envelope
import akka.actor.Props
import scala.collection.immutable.TreeMap
import net.ypmania.storage.paged.PagedStorage
import akka.actor.Stash
import net.ypmania.storage.atomic.AtomicActor._
import akka.actor.PoisonPill
import akka.actor.Terminated

class BTree(pagedStorage: ActorRef, initialRootPageIdx: PageIdx)
           (implicit val settings: BTree.Settings)
           extends Actor with Stash with ActorLogging {
  import BTree._
  
  context become active (workerActorOf(initialRootPageIdx), initialRootPageIdx)
  
  def active(root: ActorRef, rootPage: PageIdx): Receive = {
    case s: Split =>
      log.debug(s"About to split root, stopping ${root}")
      // create new root with one key
      context.watch(root)
      root ! PoisonPill
      pagedStorage ! PagedStorage.ReservePage
      context become splitting(rootPage, s)
      
    case msg =>
      root forward msg
  }
  
  def splitting(rootPage: PageIdx, s: Split): Receive = {
    var terminated = false
    var newRootPageIdx: Option[PageIdx] = None
    
    def maybeDone() {
      if (terminated && newRootPageIdx.isDefined) {
        log.debug(s"Done splitting, new root is at page ${newRootPageIdx}")
        val page = BTreePage(false, TreeMap(s.splitKey -> rootPage), s.newPageIdx)
        pagedStorage ! Atomic(PagedStorage.Write(newRootPageIdx.get -> page), atom = s.atom)
        // TODO send stash to new node
        log.warning(s"TODO handle stash ${s.stash}")
        context become active (workerActorOf(newRootPageIdx.get), newRootPageIdx.get)
        unstashAll()      
      }
    }
    
    return { 
      case Terminated(oldRoot) =>
        terminated = true
        maybeDone()
      
      case PagedStorage.PageReserved(newRoot) =>
        newRootPageIdx = Some(newRoot)
        maybeDone()
        
      case _ => stash()
    }
  }
  
  def receive = {
    case _ =>
  }
  
  def workerActorOf(page: PageIdx) = context.actorOf(
      Props(new BTreePageWorker(pagedStorage, page)), 
      page.toInt.toString)
}

object BTree {
  trait Keyed {
    def key: ID
  }
  
  case class Put (key: ID, value: PageIdx, ctx: AnyRef = None) extends Keyed
  case class PutCompleted (ctx: AnyRef)
  
  case class Get (key: ID, ctx: AnyRef = None) extends Keyed
  sealed trait GetCompleted
  case class NotFound(ctx: AnyRef) extends GetCompleted
  case class Found(value: PageIdx, ctx: AnyRef) extends GetCompleted
  
  case class Settings(order: Int) {
    require(order > 1)
    val entriesPerPage = order * 2 - 1
  }
  
  private[btree] case class Split(splitKey: ID, newPageIdx: PageIdx, stash: Vector[Envelope], atom: Atom)  
  
}