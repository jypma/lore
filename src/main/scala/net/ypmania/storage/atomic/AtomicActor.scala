package net.ypmania.storage.atomic

import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.ActorRef
import akka.pattern.ask
import akka.pattern.pipe
import java.util.concurrent.atomic.AtomicLong
import akka.util.Timeout
import akka.actor.Terminated
import akka.actor.Cancellable
import scala.concurrent.duration.Deadline
import akka.actor.Status.Failure
import akka.pattern.AskTimeoutException

class AtomicActor(target: ActorRef, implicit val timeout: Timeout) extends Actor with ActorLogging {
  import AtomicActor._
  import context.dispatcher
  
  private val messages = collection.mutable.Map.empty[Atom,QueuedMessage[_]]
  private val latestForActor = collection.mutable.Map.empty[ActorRef,Atom]
  private val watchedSenders = collection.mutable.Set.empty[ActorRef]
  private val expirations = collection.mutable.Map.empty[Atom,Cancellable] 
  
  def receive = {
    case atomic:Atomic[_] =>
      log.debug("messages: {}", messages)
      log.debug("latest  : {}", latestForActor)
      
      // TODO create separate map for this lookup
      val waitingOnSameAtoms = messages.values.filter(_.isExpectingAny(atomic.otherAtoms))
      if (waitingOnSameAtoms.isEmpty) {
    	  log.debug("Nothing waiting yet for {}", atomic.otherAtoms)
    	  // TODO create separate map for this lookup
    	  val msg = messages.values.find(_.isExpecting(atomic.atom)) match {
    	    case Some(waitingForCurrent) =>
    	      log.debug("Found msg {} which is awaiting current atom {}, merging them.", waitingForCurrent, atomic.atom)
    	      save(waitingForCurrent.merge(atomic, sender, timeout.duration.fromNow))
    	        
    	    case None =>
    	      log.debug("No msg is awaiting current atom {}", atomic.atom)
    	      atomic.queue(sender, timeout.duration.fromNow)
    	  }
    	  
    	  val msgWithPrev = latestForActor.get(sender) match {
    	    case Some(previousAtom) =>
    	      log.debug("Actor {} has previous atom {} still awaiting, blocking {}", sender, previousAtom, atomic.atom)
    	      save(msg.blocked)
    	      save(messages(previousAtom).andThen(atomic.atom))
            undeadlock(msg.blocked, previousAtom)
    	      
    	    case None =>
    	      log.debug("No previous message waiting for {}", sender)
    	      save(msg)
    	  }
    	  
	      if (msgWithPrev.canFinish) {
	        finish(msgWithPrev)
	      } 
      } else {
        log.debug("Already waiting on some of {}: {}", atomic.otherAtoms, waitingOnSameAtoms)
        val merged = waitingOnSameAtoms.reduce { (a,b) => a.merge(b) }.merge(atomic, sender, timeout.duration.fromNow)
        for (atom <- merged.received) {
          messages(atom) = merged
        }
        log.debug("Merged into {} and re-queued.", merged)
      }
      
      if (messages.contains(atomic.atom)) {
    	  latestForActor(sender) = atomic.atom
    	  watchedSenders += sender
      }
      
    case Reply(clients, msg) =>
      for (client <- clients) client ! msg
      
    case Terminated(client) =>
      latestForActor.remove(client)
      watchedSenders.remove(client)

    case Expired(atom) =>
      log.warning("Atom {} has timed out", atom)
      messages.get(atom).foreach(expire)
      
    case other =>
      target.tell(other, sender)
  }
  
  private def expire(msg: QueuedMessage[_]): Unit = {
    val reply = Failure(new AskTimeoutException("Deadline expired for atomic message " + msg))
    for (client <- msg.clients) client ! reply
    delete(msg)
    msg.next.foreach(finishNext)
  }
  
  private def watchSender():Unit = {
    if (!watchedSenders.contains(sender)) {
      context.watch(sender)
      watchedSenders += sender
    }
  }
  
  private def save(msg: QueuedMessage[_]) = {
    assume((msg.received & msg.expecting).isEmpty)

    cancelExpiration(msg)
    val atom = msg.received.head
    if (msg.deadline.hasTimeLeft) {
      expirations(atom) =  context.system.scheduler.scheduleOnce(msg.deadline.timeLeft, self, Expired(atom))
    } else {
      self ! Expired(atom)
    }
    for (atom <- msg.received) { 
      messages(atom) = msg 
    }
    
    msg
  }
  
  private def getFinishingChain(msg: QueuedMessage[_], processed: Set[Atom] = Set.empty): Seq[Atom] = {
    msg.received.toSeq ++ 
      (for (atom <- msg.next
            if !processed.contains(atom)
           ) yield getFinishingChain(messages(atom), processed ++ msg.received)
      ).flatten
  }
  
  private def undeadlock(msg: QueuedMessage[_], prev: Atom) = {
    val chain: Seq[Atom] = getFinishingChain(msg)
    val merged = chain.map(messages).reduce((a,b) => a.merge(b)).merge(msg).unblocked
    if (merged.received.contains(prev) && merged.canFinish) {
      log.warning("Resolved a deadlock involving {}", merged.clients)
      save(merged)
    } else {
      log.debug("No deadlock: {}", msg)
      msg
    }
  }
  
  private def cancelExpiration(msg: QueuedMessage[_]): Unit = {
    for (atom <- msg.received; x <- expirations.get(atom)) {
      x.cancel()
    }
  }

  private def finishNext(atom: Atom): Unit = {
    log.debug("Unblocking next message atom {}", atom)
    val msg = save(messages(atom).unblocked)
    if (msg.canFinish) finish(msg)    
  }
  
  private def finish(msg: QueuedMessage[_]): Unit = {
    log.debug("Finishing {}", msg)
    
    cancelExpiration(msg)
    import context.dispatcher
    target ? msg.msg map { Reply(msg.clients, _) } pipeTo self
    delete(msg)
    
    msg.next.foreach(finishNext)
  }

  private def delete(msg: QueuedMessage[_]) = {
    for (atom <- msg.received) {
      messages.remove(atom)
    }
    for ((actor, atom) <- latestForActor if msg.received.contains(atom)) {
      latestForActor.remove(actor)
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
  
  case class Atomic[T : Mergeable] (msg: T, atom: Atom = Atom(), otherAtoms: Set[Atom] = Set.empty) {
    private[AtomicActor] def queue(sender: ActorRef, deadline: Deadline) = 
      QueuedMessage(msg, Set(atom), otherAtoms, Seq.empty, false, Set(sender), deadline)
  }
  
  private case class QueuedMessage[T : Mergeable](
      msg: T, 
      received: Set[Atom], 
      expecting: Set[Atom], 
      next: Seq[Atom], 
      blockedByPrevious: Boolean,
      clients: Set[ActorRef],
      deadline: Deadline) {
    def merge(other: Atomic[_], sender: ActorRef, otherDeadline: Deadline) = copy(
        msg = implicitly[Mergeable[T]].merge(msg, other.msg.asInstanceOf[T]), 
        received = received + other.atom,
        expecting = other.otherAtoms ++ expecting -- received - other.atom,
        clients = clients + sender,
        deadline = if (deadline > otherDeadline) deadline else otherDeadline)
    def merge(other: QueuedMessage[_]): QueuedMessage[T] = {
      copy(
        msg = implicitly[Mergeable[T]].merge(msg, other.msg.asInstanceOf[T]),
        received = received ++ other.received,
        expecting = expecting ++ other.expecting -- other.received,
        blockedByPrevious = blockedByPrevious || other.blockedByPrevious,
        next = next.filter(atom => !other.received.contains(atom)) ++ other.next.filter(atom => !received.contains(atom)),
        clients = clients ++ other.clients,
        deadline = if (deadline > other.deadline) deadline else other.deadline)
    }
    def isExpectingAny(atoms: Set[Atom]) = !(expecting & atoms).isEmpty 
    def isExpecting(atom: Atom) = expecting.contains(atom)
    def blocked = copy(blockedByPrevious = true)
    def unblocked = copy(blockedByPrevious = false)
    def andThen(a: Atom) = copy(next = next :+ a)
    def canFinish = expecting.isEmpty && !blockedByPrevious
  }
  
  private case class Reply(clients: Set[ActorRef], msg: Any)
  
  private case class Expired(atom: Atom)
}