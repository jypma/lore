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
import scala.concurrent.Future
import akka.pattern.pipe;
import akka.actor.Stash

class FileActor(path: Path, options: Seq[OpenOption]) extends Actor with Stash with ActorLogging {
  var channel: AsynchronousFileChannel = _
  var writers = 0
  
  def receive = {
    case Ready(c, opened) =>
      channel = c
      context.become(open)
      context.parent ! opened
      
    case other => 
      log.error(s"Got ${other} from ${sender}, but not yet done opening")
      throw new IllegalStateException (s"Got ${other} from ${sender}, but not yet done opening")
  }
  
  override def preStart {
    import context.dispatcher 
    Future { 
      val opened = (if (path.toFile.exists) 
                          OpenedExisting(path.toFile.length) 
                        else 
                          OpenedNew)
      Ready(AsynchronousFileChannel.open(path, options: _*), opened)
    } pipeTo self
  }
  
  override def postStop {
    if (channel != null) {
      channel.close()      
    }
  }
  
  def open: Receive = {
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
      writers += 1
      channel.write(bytes.asByteBuffer, at, null, new CompletionHandler[Integer, Null] {
        override def completed(result: Integer, attachment: Null) {
          self ! WriteDone(replyTo, WriteCompleted(ctx))
        }
        
        override def failed(error: Throwable, attachment: Null) {
          self ! WriteDone(replyTo, Status.Failure(error))
        }
      })
      
    case WriteDone(client, response) =>
      writers -= 1
      if (writers <= 0) unstashAll()
      client ! response
      
    case Sync(ctx) =>
      if (writers > 0) {
        stash()
      } else try { 
        channel.force(true)
        log.debug("Synced")
        sender ! SyncCompleted(ctx)
      } catch {
        case x:IOException => sender ! Status.Failure(x)
      }
  }
}

object FileActor {
  def props(path: Path, options: Seq[OpenOption]) = 
    Props(classOf[FileActor], path, options)
  
  case class Read(from: Long, size: Int, ctx:AnyRef = null)
  case class ReadCompleted(bytes: ByteString, ctx:AnyRef)
  
  case class Write(at: Long, bytes: ByteString, ctx: AnyRef = null)
  case class WriteCompleted(ctx: AnyRef)
  
  case class Sync(ctx: AnyRef = null)
  case class SyncCompleted(ctx: AnyRef)
  
  sealed trait Opened
  case object OpenedNew extends Opened
  case class OpenedExisting(size: Long) extends Opened
  
  private case class Ready(channel: AsynchronousFileChannel, opened: Opened)
  private case class WriteDone(sender: ActorRef, response: AnyRef)
}