package net.ypmania.storage.atomic

import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.concurrent.Eventually
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.actor.ActorSystem
import akka.testkit.TestProbe
import akka.actor.Props
import scala.concurrent.duration._

class AtomicSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                    with WordSpecLike with Matchers with Eventually {
  import AtomicActor._
  
  case class Message(s: Set[String]) 
  object Message {
    def apply(s: String*) = new Message(s.toSet)
  }
  implicit val _ = new Mergeable[Message] {
    def merge(a: Message, b: Message) = Message(a.s ++ b.s)
  }
  
  class Fixture {
    val target = TestProbe()
    val timeout = 2.seconds
    val actor = system.actorOf(Props(new AtomicActor(target.ref, timeout)))
  }
  
  "an atomic actor" should {
    "forward a message with no other senders immediately, and forward back a reply" in new Fixture {
      val msg1 = Message("Hello, world")
      actor ! Atomic(msg1)
      target.expectMsg(msg1)
      target.reply("Hi")
      expectMsg("Hi")
    }
    
    "preserve ordering for messages from one author, even if a later message could complete immediately" in new Fixture {
      val other = TestProbe()
      val atom1 = Atom()
      actor ! Atomic(Message("Hello, world"), Set(other.ref), atom1)
      actor ! Atomic(Message("Hello, moon"))
      other.send(actor, Atomic(Message(Set("more!")), atom = atom1))
      
      target.expectMsg(Message("Hello, world", "more!"))
      target.expectMsg(Message("Hello, moon"))
    }
    
    "hold on to a message if a collaborator has another message in progress" in new Fixture {
      val other = TestProbe()
      val third = TestProbe()
      val atom1 = Atom()
      val atom2 = Atom()
      actor ! Atomic(Message("1A"), Set(other.ref), atom1)
      other.send(actor, Atomic(Message("2A"), Set(third.ref), atom2))
      other.send(actor, Atomic(Message("1B"), atom = atom1))
      third.send(actor, Atomic(Message("2B"), atom = atom2))
      
      target.expectMsg(Message("2A", "2B"))
      target.expectMsg(Message("1A", "1B"))
    }
    
    "reply to all authors when a combined message gets a reply" in new Fixture {
      val other = TestProbe()
      val atom1 = Atom()
      actor ! Atomic(Message("1A"), Set(other.ref), atom1)
      other.send(actor, Atomic(Message("1B"), atom = atom1))
      
      target.expectMsg(Message("1A", "1B"))
      target.reply("Hi")
      
      expectMsg("Hi")
      other.expectMsg("Hi")
    }
    
    "recognize a message deadlock by merging all messages involved" in new Fixture {
      val other = TestProbe()
      val atom1 = Atom()
      val atom2 = Atom()
      actor ! Atomic(Message("1A"), Set(other.ref), atom1)
      actor ! Atomic(Message("2A"), Set(other.ref), atom2)
      other.send(actor, Atomic(Message("2B"), atom = atom2))
      other.send(actor, Atomic(Message("1B"), atom = atom1))
      
      target.expectMsg(Message("1A","1B","2A","2B"))
      target.reply("Hi")
      
      expectMsg("Hi")
      other.expectMsg("Hi")      
    }
    
  }
}