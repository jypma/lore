package net.ypmania.storage.composite

import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.concurrent.Eventually
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.TestProbe
import net.ypmania.storage.paged.PageIdx
import net.ypmania.storage.paged.PagedStorage
import net.ypmania.io.IO._
import net.ypmania.lore.ID
import net.ypmania.lore.BaseID

class CompositeStoreSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                         with WordSpecLike with Matchers with Eventually {
  "a composite store" should {
    trait Fixture {
      implicit object ComposableString extends Composable[String] {
        def read(i: akka.util.ByteIterator): String = {
          val len = i.getInt
          val bytes = new Array[Byte](len)
          i.getBytes(bytes)
          new String(bytes)
        }
        
        def write(t: String,bs: akka.util.ByteStringBuilder): Unit = {
          bs.putInt(t.length)
          bs.putBytes(t.getBytes())
        }
      }
  
      val storage = TestProbe()
      val metadataPageIdx = PageIdx(0)
      val actor = system.actorOf(Props(new CompositeStore(storage.ref, metadataPageIdx)))
    }
    
    "allocate a new page when invoked with empty metadata" in new Fixture {
      val read = storage.expectMsgType[PagedStorage.Read[_]]
      read.page should be (metadataPageIdx)
      storage.reply(PagedStorage.ReadCompleted(CompositeMetadata.Type.empty))

      storage.expectMsg(PagedStorage.GetMetadata)
      storage.reply(PagedStorage.Metadata(5000, PageIdx(0)))
      
      actor ! CompositeStore.Store(BaseID(1, 1, 1), "Hello, world")
      
      storage.expectMsg(PagedStorage.ReservePage)
      storage.reply(PagedStorage.PageReserved(PageIdx(1)))
    }
    
    "store a new item in the first free page that has free space, and stores metadata afterwards" in new Fixture {
      storage.expectMsgType[PagedStorage.Read[_]]
      storage.reply(PagedStorage.ReadCompleted(CompositeMetadata(Map(PageIdx(1) -> 5, PageIdx(2) -> 5000))))
      
      storage.expectMsg(PagedStorage.GetMetadata)
      storage.reply(PagedStorage.Metadata(5000, PageIdx(3)))
      
      val id = BaseID(1, 1, 1)
      actor ! CompositeStore.Store(id, "Hello, world")
      
      val read = storage.expectMsgType[PagedStorage.Read[_]]
      read.page should be (PageIdx(2))
      storage.reply(PagedStorage.ReadCompleted(CompositeStore.Composite[String](Map.empty, Map.empty)))

      val write = storage.expectMsgType[PagedStorage.Write]
      write.pages should have size (1)
      write.pages should contain key (PageIdx(2))
      
      val content = write.pages(PageIdx(2)).content.asInstanceOf[CompositeStore.Composite[String]]
      content.storing should contain key (id)
      content.storing(id).item should be ("Hello, world")
      storage.reply(PagedStorage.WriteCompleted)
      
      val metadataWrite = storage.expectMsgType[PagedStorage.Write]
      metadataWrite.pages should have size (1)
      metadataWrite.pages should contain key (metadataPageIdx)
      
      val newMetadata = metadataWrite.pages(metadataPageIdx).content.asInstanceOf[CompositeMetadata]
      val newBytesAvailable = 5000 - 3/*ID*/ - 4/*Int(length string)*/ - "Hello, world".getBytes().length
      newMetadata should be (CompositeMetadata(Map(PageIdx(1) -> 5, PageIdx(2) -> newBytesAvailable)))
      storage.reply(PagedStorage.WriteCompleted)
      
      val complete = expectMsgType[CompositeStore.StoreCompleted]
      complete.page should be (PageIdx(2))
    }
    
    "allocate a new page when an item does not fit any free page, and have that page appear in metadata" in new Fixture {
      storage.expectMsgType[PagedStorage.Read[_]]
      storage.reply(PagedStorage.ReadCompleted(CompositeMetadata(Map(PageIdx(1) -> 5))))
      
      storage.expectMsg(PagedStorage.GetMetadata)
      storage.reply(PagedStorage.Metadata(5000, PageIdx(2)))
      
      val id = BaseID(1, 1, 1)
      actor ! CompositeStore.Store(id, "Hello, world")
      
      storage.expectMsg(PagedStorage.ReservePage)
      storage.reply(PagedStorage.PageReserved(PageIdx(2)))
      
      val read = storage.expectMsgType[PagedStorage.Read[_]]
      read.page should be (PageIdx(2))
      storage.reply(PagedStorage.ReadCompleted(CompositeStore.Composite[String](Map.empty, Map.empty)))
      
      val write = storage.expectMsgType[PagedStorage.Write]
      write.pages should have size (1)
      write.pages should contain key (PageIdx(2))
    
      val content = write.pages(PageIdx(2)).content.asInstanceOf[CompositeStore.Composite[String]]
      content.storing should contain key (id)
      content.storing(id).item should be ("Hello, world")

      storage.reply(PagedStorage.WriteCompleted)
      
      val metadataWrite = storage.expectMsgType[PagedStorage.Write]
      metadataWrite.pages should have size (1)
      metadataWrite.pages should contain key (metadataPageIdx)
      
      val newMetadata = metadataWrite.pages(metadataPageIdx).content.asInstanceOf[CompositeMetadata]
      val newBytesAvailable = 5000 - 3/*ID*/ - 4/*Int(length string)*/ - "Hello, world".getBytes().length
      newMetadata should be (CompositeMetadata(Map(PageIdx(1) -> 5, PageIdx(2) -> newBytesAvailable)))
      storage.reply(PagedStorage.WriteCompleted)
      
      val complete = expectMsgType[CompositeStore.StoreCompleted]
      complete.page should be (PageIdx(2))      
    }
    
    "try a next page if metadata about a loaded page indicates more free space than is actually there" in {
      // This might happen if a crash occurs after writing the new page content, but before the metadata.
      
    }
    
    "forget about free pages when the list does not fit in the paged storage page size anymore" in {
      pending
    }
  }
  
}