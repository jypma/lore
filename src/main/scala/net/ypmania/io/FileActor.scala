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

class FileActor(path: Path, options: Seq[OpenOption]) extends Actor {
  var channel: AsynchronousFileChannel = _
  
  val p = context.dispatcher
  
  override def preStart {
    channel = AsynchronousFileChannel.open(path, options: _*)
  }
  
  override def postStop {
    channel.close()
  }
  
  def receive = {
    case Read(from, size, context) =>
      val replyTo = sender
      val buf = ByteBuffer.allocate(size)
      channel.read(buf, from, null, new CompletionHandler[Integer,Null] {
        override def completed(result: Integer, attachment: Null) {
          replyTo ! ReadCompleted(ByteString(buf), context)
        }
        
        override def failed(error: Throwable, attachment: Null) {
          replyTo ! Status.Failure(error)
        }
      })
      
  }
}

object FileActor {
  case class Read(from: Long, size: Int, context:AnyRef = null)
  case class ReadCompleted(bytes: ByteString, context:AnyRef)
}