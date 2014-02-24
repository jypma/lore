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
    case msg @ Put(key, value, ctx) =>
      if (page.full) {
        log.debug(s"Splitting because of ${msg}")
        val (updated, key, right) = page.split
        pagedStorage ! PagedStorage.Write(pageIdx -> updated)
        pagedStorage ! PagedStorage.Create(right)
        context become splitting(updated, key, right)
        stash()
      } else if (page.internal) {
        log.debug(s"Forwarding ${msg} to a child")
        val destination = page.lookup(key)
        childForPage(destination) forward msg
      } else if (page.leaf) {
        val updated = page + (key -> value)
        log.debug(s"Writing ${updated}, replying to ${sender}")
        pagedStorage ! PagedStorage.Write(pageIdx -> updated)
        sender ! PutCompleted(ctx)
        context become active(updated)
      }      
      
    case msg @ Get(key, ctx) =>
      if (page.leaf) {
        val reply = page.get(key) match {
          case Some(value) => Found(value, ctx)
          case None => NotFound(ctx)
        }
        sender ! reply
      } else {
        val childPageIdx = page.lookup(key)
        childForPage(childPageIdx) forward msg 
      }
      
    case Split(splitKey, newPageIdx, msgs) =>
      require (!page.full)
      val updated = page + (splitKey -> newPageIdx)
      pagedStorage ! PagedStorage.Write(pageIdx -> updated)
      val child = childForPage(newPageIdx)
      for (msg <- msgs) {
        child.tell(msg.message, msg.sender)
      }
      context become active(updated)
      
  }
  
  def splitting(page: BTreePage, splitKey: ID, right: BTreePage): Receive = {
    var stashForRight = Vector.empty[Envelope]
    
    {
      case PagedStorage.CreateCompleted(rightPageIdx) =>
        context.parent ! Split(splitKey, rightPageIdx, stashForRight)
        context become active(page)
        unstashAll()
      case msg:Keyed if msg.key >= splitKey =>
        stashForRight :+= Envelope(msg, sender, context.system)
      case _ =>
        stash()
    }
  }
  
  private def childForPage(page: PageIdx) = {
    val name = page.toInt.toString
    context.child(name) match {
      case Some(actor) => 
        actor
      case None =>
        context.actorOf(Props(new BTreePageWorker(pagedStorage, page)), name)
    }
  }
}

object BTreePageWorker {
}
