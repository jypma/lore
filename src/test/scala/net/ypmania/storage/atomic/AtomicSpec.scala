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
import akka.actor.Status.Failure
import net.ypmania.test.ExtraMatchers
import akka.pattern.AskTimeoutException

class AtomicSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                    with WordSpecLike with Matchers with ExtraMatchers with Eventually {
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
    val timeout = 1.second
    val actor = system.actorOf(Props(new AtomicActor(target.ref, timeout)))
  }
  
  "an atomic actor" should {
    "forward a message with no atom immediately, and forward back a reply" in new Fixture {
      val msg1 = Message("Hello, world")
      actor ! Atomic(msg1)
      target.expectMsg(msg1)
      target.reply("Hi")
      expectMsg("Hi")
    }
    
    "forward a message with no other atoms immediately, and forward back a reply" in new Fixture {
      val msg1 = Message("Hello, world")
      actor ! Atomic(msg1, atom = Atom())
      target.expectMsg(msg1)
      target.reply("Hi")
      expectMsg("Hi")
    }
    
    "preserve ordering for messages from one author, even if a later message could complete immediately" in new Fixture {
      val other = TestProbe()
      val atom1, atom2 = Atom()
      actor ! Atomic(Message("Hello, world"), atom1, Set(atom2))
      actor ! Atomic(Message("Hello, moon"))
      other.send(actor, Atomic(Message(Set("more!")), atom2, Set(atom1)))
      
      target.expectMsg(Message("Hello, world", "more!"))
      target.expectMsg(Message("Hello, moon"))
    }
    
    "hold on to a message if a collaborator has another message in progress" in new Fixture {
      val other = TestProbe()
      val third = TestProbe()
      val atom1a, atom1b, atom2a, atom2b = Atom()
      
      actor ! Atomic(Message("1A"), atom1a, Set(atom1b))
      other.send(actor, Atomic(Message("2A"), atom2a, Set(atom2b)))
      other.send(actor, Atomic(Message("1B"), atom = atom1b))
      third.send(actor, Atomic(Message("2B"), atom = atom2b))
      
      target.expectMsg(Message("2A", "2B"))
      target.expectMsg(Message("1A", "1B"))

      val atom3a, atom3b, atom4a, atom4b = Atom()
      actor ! Atomic(Message("3A"), atom3a, Set(atom3b))
      other.send(actor, Atomic(Message("4A"), atom4a, Set(atom4b)))
      other.send(actor, Atomic(Message("3B"), atom = atom3b))
      third.send(actor, Atomic(Message("4B"), atom = atom4b))
      
      target.expectMsg(Message("4A", "4B"))
      target.expectMsg(Message("3A", "3B"))
    }
    
    "reply to all authors when a combined message gets a reply" in new Fixture {
      val other = TestProbe()
      val atom1a, atom1b = Atom()
      actor ! Atomic(Message("1A"), atom1a, Set(atom1b))
      other.send(actor, Atomic(Message("1B"), atom = atom1b))
      
      target.expectMsg(Message("1A", "1B"))
      target.reply("Hi")
      
      expectMsg("Hi")
      other.expectMsg("Hi")
    }
    
    "recognize a message deadlock by merging all messages involved" in new Fixture {
      val other = TestProbe()
      val atom1a, atom1b, atom2a, atom2b = Atom()
      actor ! Atomic(Message("1A"), atom1a, Set(atom1b))
      actor ! Atomic(Message("2A"), atom2a, Set(atom2b))
      other.send(actor, Atomic(Message("2B"), atom = atom2b))
      other.send(actor, Atomic(Message("1B"), atom = atom1b))
      
      target.expectMsg(Message("1A","1B","2A","2B"))
      target.reply("Hi")
      
      expectMsg("Hi")
      other.expectMsg("Hi")      
    }
    
    "forward non-atomic messages and their replies unchanged" in new Fixture {
      actor ! "Hello"
      target expectMsg "Hello"
      target reply "World"
      expectMsg("World")
    }
    
    "release two senders blocking on the same atom" in new Fixture {
      val atom1, atom2 = Atom()
      val other = TestProbe()
      actor ! Atomic(Message("Hello from spec"), atom1, Set(atom2))
      other.send(actor, Atomic(Message("Hello from other"), atom1, Set(atom2)))
      actor ! Atomic(Message("Releasing"), atom = atom2)
      
      target.expectMsg(Message("Hello from spec", "Hello from other", "Releasing"))
      target.reply("Done")
      
      expectMsg("Done")
      other.expectMsg("Done")
    }
    
    "time out if an expected atom for a blocked message never arrives" in new Fixture {
      val atom1, atom2 = Atom()
      actor ! Atomic(Message("Hello"), atom1, Set(atom2))

      // We don't send anything with atom2 on it, so the above message should time out.
      val failed = expectMsgType[Failure]
      failed.cause should (beOfType[AskTimeoutException])
      
      // If the second message finally arrives but too late, it's OK that we have forgotten
      // about atom1 by now, so this is expected to time out as well.
      actor ! Atomic(Message("World"), atom2, Set(atom1))
      expectMsgType[Failure]
    }
    
    "normally complete a message that was blocked to preserve ordering, if a previous message times out" in new Fixture {
      val other = TestProbe()
      val atom1, atom2 = Atom()
      actor ! Atomic(Message("Hello, world"), atom1, Set(atom2))
      actor ! Atomic(Message("Hello, moon"))
      
      // We don't send anything with atom2 on it, so the above message should time out.
      val failed = expectMsgType[Failure]
      
      // But afterwards, the second message should be delivered normally.
      target.expectMsg(Message("Hello, moon"))
    }
  }
}