package net.ypmania.lore

import java.nio.file.Paths
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.WRITE
import akka.actor.Actor
import akka.actor.Props
import net.ypmania.io.FileActor
import akka.actor.ActorRef
import java.nio.ByteOrder
import akka.actor.Status
import akka.util.ByteString

class PagedFile(file: ActorRef, header: PagedFile.Header) extends Actor {
  import PagedFile._
  
  def receive = {
    case Read(page, pageType) =>
      context.actorOf(Props(new Reader(sender, page, pageType)))      
  }
  
  class Reader(requestor: ActorRef, page: Int, pageType: PageType[_]) extends Actor {
    override def preStart {
      file ! FileActor.Read(page * header.pageSize, header.pageSize)
    }
    
    def receive = {
      case FileActor.ReadCompleted(bytes, _) =>
        requestor ! pageType.read(bytes)
    }
  }
}

object PagedFile {
  trait PageType[T] {
    def read(page: ByteString): T
    def write(page: T): ByteString
  }
  
  case class Open(filename: String)
  case class Read(page: Int, pageType: PageType[_])
  
  val magic = "lore-db"
  val magicBytes = magic.getBytes("UTF-8")
  val headerSize = magicBytes.length + 16
  val defaultPageSize = 64 * 1024
  
  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN
  
  class IO extends Actor {    
    def receive = {
      // Actually, launch an actor per "stateful" request, i.e. opening up a paged file and reading all of its required data.
      // Then we can later proxy the PagedFile to support restarts of the underlying file.
      //   (If we even need to; maybe the actor creating the PagedFile can just restart it itself)
      
      case Open(filename) =>
        import java.nio.file.StandardOpenOption._
        val file = context.actorOf(Props(new FileActor(Paths.get(filename), Seq(READ, WRITE, CREATE))))
        context.actorOf(Props(new Opener(sender, file)))
    }

    class Opener(requestor: ActorRef, file: ActorRef) extends Actor {
      case object ReadHeader

      override def preStart {
        file ! FileActor.Read(0, headerSize, ReadHeader)
      }

      def receive = {
        case FileActor.ReadCompleted(bytes, ReadHeader) =>
          val i = bytes.iterator
          val fileMagic = new Array[Byte](magic.length);
          i.getBytes(fileMagic);
          if (fileMagic != magicBytes)
            requestor ! Status.Failure(new Exception("missing file magic"))
          val header = Header(i.getInt, i.getInt, i.getInt, i.getInt)
          if (header.pageSize != defaultPageSize)
            requestor ! Status.Failure(new Exception(s"expected page size $defaultPageSize, got ${header.pageSize}"))
          val pagedFile = context.actorOf(Props(new PagedFile(file, header)))
          requestor ! pagedFile
      }
    }
  }
  
  case class Header(pageSize: Int, firstFreePage: Int, branchesPage: Int, commandsPage: Int)
  
  
}