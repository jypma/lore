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
import net.ypmania.storage.btree.BTreePage.SplitResult

object BTree {
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