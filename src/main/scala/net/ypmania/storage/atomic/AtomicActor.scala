package net.ypmania.storage.atomic

import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.ActorRef
import akka.pattern.ask
import akka.pattern.pipe
import java.util.concurrent.atomic.AtomicLong
import akka.util.Timeout

class AtomicActor(target: ActorRef, implicit val timeout: Timeout) extends Actor with ActorLogging {
  import AtomicActor._
  private var messages = Map.empty[Atom,QueuedMessage[_]]
  private var latestForActor = Map.empty[ActorRef,Atom]
  
  def receive = {
    case atomic:Atomic[_] =>
      val inProgress = messages.get(atomic.atom)
      val current = inProgress.map(_.merge(atomic, sender)).getOrElse(atomic.queue(sender))
      messages += (atomic.atom -> current)          
      
      latestForActor.get(sender) map { atom =>
        log.debug(s"Holding on to ${atomic.atom}:${current} from ${sender}, since ${atom} is already in progress")
        
        messages += (atom -> messages(atom).andThen(atomic.atom))
        messages += (atomic.atom -> current.after(atom))
        
        val deps = finishableDependenciesFrom(atomic.atom)
        if (!deps.isEmpty) mergeAndFinish(deps)
      } getOrElse {
        if (current.canFinish) {  
          finish(atomic.atom)
        } else {
          log.debug(s"Holding on to ${atomic.atom}:${current} from ${sender} since it can't complete yet.")
          
          latestForActor += (sender -> atomic.atom)
        }
      }
      
    case Reply(clients, msg) =>
      for (client <- clients) client ! msg
      
    case other =>
      import context.dispatcher
      target ? other pipeTo sender
  }
  
  def finishableDependenciesFrom (top: Atom): Set[Atom] = finishableDependenciesFrom (Set(top), top)
  
  def finishableDependenciesFrom (deps: Set[Atom], top: Atom): Set[Atom] = {
    val msg = messages(top)
    log.debug(s"Checking for finishable ${deps} on ${top}:${msg}")
    if (deps.isEmpty) Set.empty
    else if (!msg.expecting.isEmpty) Set.empty
    else if (msg.prev.isEmpty) deps & Set(top)
    else {
      val newDeps = deps ++ msg.prev
      val toCheckDeps = msg.prev -- deps
      (newDeps /: toCheckDeps)(finishableDependenciesFrom(_,_))
    }
  }
  
  def mergeAndFinish(atoms: Set[Atom]) {
    log.debug (s"Merging and finishing ${atoms}")
    val top = atoms.head
    val deps = atoms.tail
    val msg = (messages(top) /: deps)((msg, atom) => msg.merge(messages(atom))).independently
    messages += (top -> msg)
    deps foreach { messages -= _ }
    finish(top)
  }
  
  private def finish(atom: Atom): Unit = {
    val present = messages.get(atom)
    if (present.isEmpty) {
      log.debug(s"${atom} apparently already finished, skipping.")
      return
    }
    val msg = present.get
    log.debug(s"Finishing ${atom}:${msg}")
    
    import context.dispatcher
    target ? msg.msg map { Reply(msg.received, _) } pipeTo self
    messages -= atom
    
    for (next <- msg.next) {
      val msg = messages(next).afterFinished(atom)
      messages += next -> msg
      if (msg.canFinish) finish(next)
    }
  }
}

object AtomicActor {
  trait Mergeable[T] {
    def merge(a: T, b: T): T
  }
  
  case class Atom private (val value: Long) extends AnyVal
  object Atom {
    private val counter = new AtomicLong
    def apply() = new Atom(counter.incrementAndGet())
  }
  
  case class Atomic[T : Mergeable] (msg: T, otherSenders: Set[ActorRef] = Set.empty, atom: Atom = Atom()) {
    private[AtomicActor] def queue(sender: ActorRef) = 
      QueuedMessage(msg, Set(sender), otherSenders, Set.empty, Set.empty)
  }
  
  private case class QueuedMessage[T : Mergeable](
      msg: T, received: Set[ActorRef], expecting: Set[ActorRef], 
      next: Set[Atom], prev: Set[Atom]) {
    def merge(other: Atomic[_], sender: ActorRef) = copy(
        msg = implicitly[Mergeable[T]].merge(msg, other.msg.asInstanceOf[T]), 
        received = received + sender,
        expecting = other.otherSenders ++ expecting -- received - sender)
    def merge(other: QueuedMessage[_]) = copy(
        msg = implicitly[Mergeable[T]].merge(msg, other.msg.asInstanceOf[T]),
        received = received ++ other.received,
        expecting = expecting ++ other.expecting,
        prev = prev ++ other.prev,
        next = next ++ other.next)
    def andThen(a: Atom) = copy(next = next + a)
    def after(a: Atom) = copy(prev = prev + a)
    def afterFinished(a: Atom) = copy(prev = prev - a)
    def independently = copy(prev = Set.empty, next = Set.empty)
    def canFinish = expecting.isEmpty && prev.isEmpty
  }
  
  private case class Reply(clients: Set[ActorRef], msg: Any)
}