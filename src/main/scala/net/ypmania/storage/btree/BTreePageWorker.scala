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

class BTreePageWorker(pagedStorage: ActorRef, pageIdx: PageIdx)
                     (implicit val settings: BTree.Settings) 
                     extends Actor with Stash with ActorLogging {
  
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
        pagedStorage ! PagedStorage.ReservePage
        stash()
        context become reservingForSplit(page)
      } else page match {
        case internal:InternalBTreePage =>
          log.debug(s"Forwarding ${msg} to a child")
          val destination = internal.lookup(key)
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
          val childPageIdx = internal.lookup(key)
          childForPage(childPageIdx) forward msg 
        
        case leaf:LeafBTreePage =>
          val reply = leaf.get(key) match {
            case Some(value) => Found(value)
            case None => NotFound
          }
          sender ! reply
      }
      
    case Split(info, msgs, atom) =>
      page match {
        case internal:InternalBTreePage if !internal.full =>
          val updated = internal.splice(info)
          pagedStorage ! Atomic(PagedStorage.Write(pageIdx -> updated), atom = atom, otherSenders = Set(sender))
          val child = childForPage(info.rightPageIdx)
          for (msg <- msgs) {
            log.debug (s"Split, forwarding $msg")
            child.tell(msg.message, msg.sender)
          }          
          context become active(updated)
      }
  }
  
  def reservingForSplit(page: BTreePage): Receive = {
    case PagedStorage.PageReserved(rightPageIdx) =>
      val (left, right, splitResult) = page.split(pageIdx, rightPageIdx)
      log.debug(s"Got reservation for new right page $rightPageIdx, splitting into $splitResult")
      unstashAll()
      self ! RedeliverForSplitCompleted
      context become redeliveringForSplit(left, right, splitResult, Vector.empty)
    case other =>
      log.debug(s"reserving: stashing $other")
      stash()
  }
  
  def redeliveringForSplit(left: BTreePage, right: BTreePage, splitResult: SplitResult, stashForRight: Vector[Envelope]): Receive = {
    case msg:Keyed if msg.key >= splitResult.key =>
      log.debug(s"Stashing for new right node: $msg")
      context become redeliveringForSplit (left, right, splitResult, stashForRight :+ Envelope(msg, sender, context.system))
      
    case RedeliverForSplitCompleted =>
      log.debug("Redeliveries done, completing split")
      val atom = Atom()
      pagedStorage ! Atomic(
          PagedStorage.Write(pageIdx -> left) + (splitResult.rightPageIdx -> right),
          otherSenders = Set(context.parent),
          atom = atom)
      context.parent ! Split(splitResult, stashForRight, atom)
      context become active(left)
      unstashAll()
      
    case other =>
      log.debug(s"redelivering: stashing $other")
      stash()
  }
  
  def childForPage(page: PageIdx) = {
    val name = page.toInt.toString
    context.child(name) getOrElse context.actorOf(Props(new BTreePageWorker(pagedStorage, page)), name)
  }
}

object BTreePageWorker {
  private object RedeliverForSplitCompleted
}
