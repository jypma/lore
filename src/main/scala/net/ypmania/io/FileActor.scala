package net.ypmania.io

import akka.actor.Actor
import java.nio.file.Path
import java.nio.file.OpenOption
import java.nio.channels.AsynchronousFileChannel
import FileActor._
import akka.util.ByteString
import java.nio.ByteBuffer
import java.nio.channels.CompletionHandler
import akka.actor.Status
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ActorLogging
import java.io.IOException

class FileActor(path: Path, options: Seq[OpenOption]) extends Actor with ActorLogging {
  var channel: AsynchronousFileChannel = _
  
  override def preStart {
    channel = AsynchronousFileChannel.open(path, options: _*)
  }
  
  override def postStop {
    channel.close()
  }
  
  def receive = {
    case Read(from, size, ctx) =>
      log.debug("Reading {} bytes at {}", size, from)
      val replyTo = sender
      val buf = ByteBuffer.allocate(size)
      channel.read(buf, from, null, new CompletionHandler[Integer,Null] {
        override def completed(result: Integer, attachment: Null) {
          buf.rewind()
          log.debug("Got {} bytes, result {}", buf.remaining(), result)
          replyTo ! ReadCompleted(ByteString(buf), ctx)
        }
        
        override def failed(error: Throwable, attachment: Null) {
          replyTo ! Status.Failure(error)
        }
      })
      
    case Write(at, bytes, ctx) =>
      log.debug("Writing {} bytes at {}", bytes.asByteBuffer.remaining(), at)
      val replyTo = sender
      channel.write(bytes.asByteBuffer, at, null, new CompletionHandler[Integer, Null] {
        override def completed(result: Integer, attachment: Null) {
          replyTo ! WriteCompleted(ctx)
        }
        
        override def failed(error: Throwable, attachment: Null) {
          replyTo ! Status.Failure(error)
        }        
      })
      
    case Sync(ctx) =>
      try { 
        channel.force(true)
        sender ! SyncCompleted(ctx)
      } catch {
        case x:IOException => sender ! Status.Failure(x)
      }
  }
}

object FileActor {
  case class Read(from: Long, size: Int, ctx:AnyRef = null)
  case class ReadCompleted(bytes: ByteString, ctx:AnyRef)
  
  case class Write(at: Long, bytes: ByteString, ctx: AnyRef = null)
  case class WriteCompleted(ctx: AnyRef)
  
  case class Sync(ctx: AnyRef = null)
  case class SyncCompleted(ctx: AnyRef)
  
  class IO extends Actor with ActorLogging {
    import IO._
    def receive = {
      case Open(path, options, ctx) =>
        val exists = path.toFile.exists
        log.debug(s"Opening ${path}, exists: ${exists}")
        val file = context.actorOf(Props(new FileActor(path, options)))
        sender ! (if (exists) 
                    OpenedExisting(file, path.toFile.length, ctx)
                  else
                    OpenedNew(file, ctx))
    }
  }
  
  object IO {
    case class Open(path: Path, options: Seq[OpenOption], ctx:AnyRef = null)
    sealed trait Opened {
      def file: ActorRef
      def ctx: AnyRef
    }
    case class OpenedNew(file: ActorRef, ctx: AnyRef) extends Opened
    case class OpenedExisting(file: ActorRef, size: Long, ctx: AnyRef) extends Opened    
  }
}