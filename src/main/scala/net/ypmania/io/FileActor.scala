package net.ypmania.io

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.CompletionHandler
import java.nio.file.OpenOption
import java.nio.file.Path

import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Stash
import akka.pattern.pipe
import akka.util.ByteString

class FileActor(path: Path, options: Seq[OpenOption]) extends Actor with Stash with ActorLogging {
  import FileActor._
  
  var channel: AsynchronousFileChannel = _
  var writers: Int = 0
  var state: State = _
  
  def receive = {
    case Ready(c, newState) =>
      channel = c
      state = newState
      writers = 0
      unstashAll()
      context.become(open)
      
    case _ => 
      stash()
  }
  
  override def preStart {
    import context.dispatcher 
    Future { 
      val state = (if (path.toFile.exists) 
                          Existing(path.toFile.length) 
                        else 
                          New)
      Ready(AsynchronousFileChannel.open(path, options: _*), state)
    } pipeTo self
  }
  
  override def postStop {
    if (channel != null) {
      channel.close()      
    }
  }
  
  def open: Receive = {
    case GetState =>
      sender ! state
    
    case Read(from, size) =>
      log.debug("Reading {} bytes at {}", size, from)
      val replyTo = sender
      val buf = ByteBuffer.allocate(size)
      channel.read(buf, from, null, new CompletionHandler[Integer,Null] {
        override def completed(result: Integer, attachment: Null) {
          buf.rewind()
          log.debug("Got {} bytes, result {}", buf.remaining(), result)
          val bytes = ByteString(buf)
          log.debug(s"Length ${bytes.length}")
          val reply = if (result >= bytes.length) {
            bytes
          } else {
            bytes.take(result)
          }
          replyTo ! ReadCompleted(reply)
        }
        
        override def failed(error: Throwable, attachment: Null) {
          self ! Failure(error)
        }
      })
      
    case Write(at, bytes) =>
      val size = bytes.asByteBuffer.remaining()
      log.debug("Writing {} bytes at {}", size, at)
      state = state.growTo(at + size)
      val replyTo = sender
      writers += 1
      channel.write(bytes.asByteBuffer, at, null, new CompletionHandler[Integer, Null] {
        override def completed(result: Integer, attachment: Null) {
          self ! WriteDone(replyTo, Success(WriteCompleted))
        }
        
        override def failed(error: Throwable, attachment: Null) {
          self ! WriteDone(replyTo, Failure(error))
        }
      })
      
    case WriteDone(client, result) =>
      writers -= 1
      if (writers <= 0) unstashAll()
      result match {
        case Success(msg) => client ! msg
        case failed:Failure[_] => self ! failed
      }
      
    case Sync =>
      if (writers > 0) {
        stash()
      } else {
        // Should this be in a Future { } block since it's blocking?
        channel.force(true)
        log.debug("Synced")
        sender ! SyncCompleted
      } 

    case Failure(error) =>
      throw error
  }
}

object FileActor {
  def props(path: Path, options: Seq[OpenOption]) = 
    Props(classOf[FileActor], path, options)
  
  case class Read(from: Long, size: Int)
  case class ReadCompleted(bytes: ByteString)
  
  case class Write(at: Long, bytes: ByteString)
  case object WriteCompleted
  
  case object Sync
  case object SyncCompleted
  
  case object GetState
  sealed trait State {
    def growTo(size: Long): Existing
  }
  case object New extends State {
    def growTo(size: Long) = new Existing(size)
  }
  case class Existing(size: Long) extends State {
    def growTo(newSize: Long) = if (newSize <= size) this else new Existing(newSize)
  }
  
  private case class Ready(channel: AsynchronousFileChannel, state: State)
  private case class WriteDone(sender: ActorRef, result:Try[WriteCompleted.type])
}