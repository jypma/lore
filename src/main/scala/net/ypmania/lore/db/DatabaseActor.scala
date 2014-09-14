package net.ypmania.lore.db

import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.ActorRef
import net.ypmania.storage.paged.PagedStorage
import net.ypmania.storage.paged.PageIdx
import akka.pattern.ask
import akka.actor.Stash
import net.ypmania.storage.btree.BTreePage
import net.ypmania.lore.event.EventsPage
import akka.util.Timeout
import concurrent.duration._
import net.ypmania.storage.btree.BTree
import akka.actor.Props

class DatabaseActor(storage: ActorRef) extends Actor with Stash with ActorLogging {
  import MetadataPage._
  import BTreePage._
  import EventsPage._
  implicit val timeout = Timeout(1.minute)
  import context._
  
  storage ! PagedStorage.Read[MetadataPage](PageIdx(0))
  
  def receive = { 
    case PagedStorage.ReadCompleted(metadata: MetadataPage) =>
      context become initializing(metadata)
    case _ =>
      stash()
  }
  
  def initializing(metadata: MetadataPage): Receive = {
    implicit val btreeSettings = /*dataHeader.btreeSettings*/ BTree.Settings(12)
    //val branchesBTree = context.actorOf(Props(new BTree(storage, metadata.branchesIndex)))
    
    // TODO rename BTree to BTreeActor?
    // TODO make EventsActor? that encapsulates EventsPage
    {
      case _ =>
        
    }
  }
  
  def ready(metadata: MetadataPage) {
    
  }
}