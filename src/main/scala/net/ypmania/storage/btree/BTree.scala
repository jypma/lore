package net.ypmania.storage.btree

import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.ActorRef
import net.ypmania.storage.paged.PageIdx
import net.ypmania.lore.ID
import akka.dispatch.Envelope
import akka.actor.Props
import scala.collection.immutable.TreeMap
import net.ypmania.storage.structured.StructuredStorage
import akka.actor.Stash

class BTree(structuredStorage: ActorRef, initialRootPageIdx: PageIdx)
           (implicit val settings: BTree.Settings)
           extends Actor with Stash with ActorLogging {
  import BTree._
  
  def active(root: ActorRef, rootPage: PageIdx): Receive = {
    case s: Split =>
      // create new root with one key
      context.stop(root)
      val page = BTreePage(false, TreeMap(s.splitKey -> rootPage), s.newPageIdx)
      structuredStorage ! StructuredStorage.Create(page)
      context become splitting(rootPage, s)
      
    case msg =>
      root forward msg
  }
  
  def splitting(rootPage: PageIdx, s: Split): Receive = {
    case StructuredStorage.CreateCompleted(newRootPageIdx, _) =>
      context become active (workerActorOf(newRootPageIdx), newRootPageIdx)
      unstashAll()
      
    case _ => stash()
  }
  
  context become active (workerActorOf(initialRootPageIdx), initialRootPageIdx)
  
  def receive = {
    case _ =>
  }
  
  def workerActorOf(page: PageIdx) = context.actorOf(
      Props(new BTreePageWorker(structuredStorage, page)), 
      page.toString)
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
  
  private[btree] case class Split(splitKey: ID, newPageIdx: PageIdx, stash: Vector[Envelope])  
  
}