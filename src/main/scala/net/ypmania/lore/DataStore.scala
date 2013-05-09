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
    override def preStart {
      context.actorOf(Props(new PagedFile.Opener(self, filename)))
    }
    
    def receive = {
      case pagedFile:ActorRef =>
        
      case other =>
        log.error("Dropping: {}", other)
    }
  }
}