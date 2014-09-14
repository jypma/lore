package net.ypmania.test

import akka.testkit.TestProbe
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import akka.testkit.TestKit
import akka.actor.ActorRef

class ParentingTestProbe(childProps: Props)(implicit system: ActorSystem) extends TestProbe(system) {
  root =>
  
  val actor = system.actorOf(Props(new Actor {
    val child = context.actorOf(childProps, "c")
    ParentingTestProbe.this.ref ! child
    def receive = {
      case x => ParentingTestProbe.this.ref forward x
    }
  }))
    
  val child = expectMsgType[ActorRef]
}

object ParentingTestProbe {
  def apply(childProps: Props)(implicit system: ActorSystem) = new ParentingTestProbe(childProps)
}