package net.ypmania.lore.event

import akka.actor.Stash
import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.ActorRef

class EventStore(storage: ActorRef) extends Actor with Stash with ActorLogging {
  def receive = {
    case _ =>
  }
}