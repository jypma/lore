package net.ypmania.storage.btree

import net.ypmania.lore.ID
import net.ypmania.storage.atomic.AtomicActor._
import net.ypmania.storage.paged.PageIdx

import akka.actor.ActorRef
import akka.actor.Props

object BTree {
  def props(pagedStorage: ActorRef, rootPageIdx: PageIdx)(implicit settings: BTree.Settings) =
    Props(new BTreePageWorker(pagedStorage, rootPageIdx, true))
  
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