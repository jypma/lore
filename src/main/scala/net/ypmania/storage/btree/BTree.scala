package net.ypmania.storage.btree

import net.ypmania.lore.ID
import net.ypmania.storage.atomic.AtomicActor._
import net.ypmania.storage.paged.PageIdx
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.Terminated
import akka.actor.PoisonPill
import net.ypmania.storage.paged.PagedStorage
import akka.actor.Stash

class BTree(pagedStorage: ActorRef, rootPageIdx: PageIdx)(implicit settings: BTree.Settings) extends Actor with Stash with ActorLogging {
  import BTree._
  
  var parentForActor: Map[ActorRef, PageIdx] = Map.empty
  var pageForActor: Map[ActorRef, PageIdx] = Map.empty
  var root = childForPage(rootPageIdx)

  def receive = { 
    case msg:ApplySplit  =>
      childForPage(parentForActor(sender)) forward msg
      
    case ToChild(page, msg) =>
      childForPage(page) forward msg
      
    case Terminated(actor) =>
      pageForActor -= actor
      parentForActor -= actor
      
    case msg =>
      root forward msg
  }
  
  def childForPage(page: PageIdx) = {
    val name = page.toInt.toString
    context.child(name) getOrElse {
      val parent = pageForActor.get(sender)
      val actor = context.actorOf(Props(new BTreePageWorker(pagedStorage, page, isRoot = parent.isEmpty)), name)
      pageForActor += actor -> page
      parent foreach { p => parentForActor += actor -> p } 
      context.watch(actor)
      actor
    }
  }  
}

object BTree {
  def props(pagedStorage: ActorRef, rootPageIdx: PageIdx)(implicit settings: BTree.Settings) =
    Props(new BTree(pagedStorage, rootPageIdx))
  
  private[btree] case class ToChild[T](page: PageIdx, msg:T)
  private[btree] case class ApplySplit(info: BTreePage.SplitResult, atom: Atom, otherAtoms: Set[Atom])
    
  trait Keyed {
    def key: ID
  }
  
  case class Put (key: ID, value: PageIdx) extends Keyed
  case object PutCompleted
  
  case class Get (key: ID) extends Keyed
  sealed trait GetCompleted
  case object NotFound extends GetCompleted
  case class Found(value: PageIdx) extends GetCompleted
  
  case class Settings(order: Int) {
    require(order > 1)
    val entriesPerPage = order * 2 - 1
  }
  
}