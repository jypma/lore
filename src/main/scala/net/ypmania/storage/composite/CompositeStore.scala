package net.ypmania.storage.composite

import akka.actor.Stash
import akka.actor.ActorLogging
import akka.actor.Actor
import akka.actor.ActorRef
import net.ypmania.storage.atomic.AtomicActor.Mergeable
import net.ypmania.storage.paged.PageIdx
import net.ypmania.storage.paged.PagedStorage
import net.ypmania.lore.ID
import akka.util.ByteStringBuilder
import akka.util.ByteIterator
import akka.util.ByteString
import net.ypmania.io.IO._
import akka.util.Timeout
import scala.concurrent.duration._
import akka.pattern.ask
import akka.pattern.pipe

class CompositeStore (storage: ActorRef, metadataIdx: PageIdx) extends Actor with Stash with ActorLogging {
  import CompositeStore._
  import context.dispatcher
  implicit val timeout = Timeout(1.minute)
  
  case class Initialized(metadata: CompositeMetadata, storageMetadata: PagedStorage.Metadata)
  
  log.info(s"Asking $storage for metadata page $metadataIdx")
  
  (storage ? PagedStorage.Read[CompositeMetadata](metadataIdx)) zip 
  (storage ? PagedStorage.GetMetadata) pipeTo self
  
  def receive = {
    case (PagedStorage.ReadCompleted(metadata: CompositeMetadata), storageMetadata: PagedStorage.Metadata) =>
      log.debug(s"Received md $metadata, ready now.")
      context become ready(metadata, storageMetadata)
      unstashAll()
    case _ =>
      stash()
  }
  
  def ready(metadata: CompositeMetadata, storageMetadata: PagedStorage.Metadata): Receive = {
    {
      case msg:Store[_] =>
        val bytes = msg.asByteString
        val client = sender
        metadata.pageWithSpace(bytes.size) match {
          case Some(page) =>
            log.debug(s"Storing ${bytes.length} bytes into known page ${page}")
            self ! msg.into(page, client)
          case None =>
            log.info("Reserving a new page.")
            storage ? PagedStorage.ReservePage map {
              case PagedStorage.PageReserved(page) =>
                log.debug(s"Storing ${bytes.length} bytes into newly reserved page $page")
                msg.into(page, client)
            } pipeTo self
        }
        
      case msg:StoreInto[_] =>
        log.debug(s"Proceeding to store into ${msg.page}")
        storage ? msg.readPage map msg.onRead pipeTo self
        
      case msg:WriteBack[_] =>
        log.debug(s"Writing back page ${msg.page}")
        val newMetadata = metadata alloc (msg.page, msg.content.bytesStoring, storageMetadata.pageSize)
        log.debug(s"And metadata page ${metadataIdx}, md now is ${metadata}")
        storage ? (msg.writePage + (metadataIdx -> newMetadata)) map { 
          case PagedStorage.WriteCompleted => StoreCompleted(msg.page) 
        } pipeTo msg.client 
        context become ready(newMetadata, storageMetadata)
    }
  }
}

object CompositeStore {
  case class Store[T: Composable](id: ID, item: T) {
    lazy val asByteString = {
      val bs = ByteString.newBuilder
      id.write(bs)
      implicitly[Composable[T]].write(item, bs)
      bs.result
    }
    private[CompositeStore] def into(page: PageIdx, client: ActorRef) = StoreInto(page, this, client)
  }
  case class StoreCompleted(page: PageIdx)
  
  case class Composite[T: Composable](loaded: Map[ID,T], storing: Map[ID,Store[T]]) {
    def size = loaded.size + storing.size
    def + (item: Store[T]) = copy (storing = storing + (item.id -> item))
    def bytesStoring = storing.values.map(_.asByteString.length).sum
  }
  
  implicit def compositePageType[T: Composable] = new PagedStorage.PageType[Composite[T]] {
    val composable = implicitly[Composable[T]]
    
    def fromByteString(page: ByteString) = {
      val i = page.iterator
      val count = i.getInt
      val items = Map.newBuilder[ID,T]
      for (idx <- 0 until count) {
        items += ID(i) -> composable.read(i)
      }
      Composite(items.result, Map.empty)
    }
    
    def toByteString(page: Composite[T]) = {
      val bs = ByteString.newBuilder
      bs.putInt(page.size) // can't use varint here, since free space must be known
      for ((id, item) <- page.loaded) {
        id.write(bs)
        composable.write(item, bs)
      }
      for (store <- page.storing.values) {
        bs ++= store.asByteString
      }
      bs.result
    }
    
    def empty = Composite(Map.empty, Map.empty)    
  }

  private case class StoreInto[T: Composable](page: PageIdx, cmd: Store[T], client: ActorRef) {
    def readPage = PagedStorage.Read[Composite[T]](page)
    def onRead: Any => WriteBack[T] = { 
      case PagedStorage.ReadCompleted(content: Composite[T]) => WriteBack(page, content + cmd, client)
    }
  }
  
  private case class WriteBack[T: Composable](page: PageIdx, content: Composite[T], client: ActorRef) {
    def writePage(implicit author: ActorRef) = PagedStorage.Write(page -> content)
  }
}