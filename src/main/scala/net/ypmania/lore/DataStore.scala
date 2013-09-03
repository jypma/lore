package net.ypmania.lore

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props

class DataStore(pagedFile: ActorRef) extends Actor with ActorLogging {
  def receive = {
      case other =>
        log.error("Dropping: {}", other)
    
  }
}

object DataStore {
  class Opener(requestor: ActorRef, filename: String) extends Actor with ActorLogging {
    private var pagedFile: ActorRef = _
    
    override def preStart {
      context.actorOf(Props(classOf[PagedFile.Opener], self, filename))
    }
    
    def receive = {
      case pagedFile:ActorRef =>
        this.pagedFile = pagedFile
        pagedFile ! PagedFile.Read(PageIdx(0), MetadataPage.Type)
        
      //case PagedFile.PageNotFound(_) =>
      //  val meta = MetadataPage.emptyDb
      //  val free = FreeListPage.empty
      //  val branchesIdx = BTreeNodePage.empty
      //  val commandsIdx = BTreeNodePage.empty 
        
        // build empty btree for commands etc., and metadata page, and store.
        
      case PagedFile.ReadCompleted(meta: MetadataPage, _) =>
        
      case other =>
        log.error("Dropping: {}", other)
    }
  }
}