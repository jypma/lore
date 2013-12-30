package net.ypmania.storage.structured

import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.concurrent.Eventually
import org.scalatest.Matchers
import org.scalatest.WordSpecLike
import akka.actor.ActorSystem
import akka.testkit.TestProbe
import akka.testkit.TestActorRef
import akka.actor.Props
import akka.util.ByteString
import net.ypmania.storage.paged.PageIdx
import net.ypmania.storage.paged.PagedStorage

class StructuredStorageSpec extends TestKit(ActorSystem("Test")) with ImplicitSender 
                       with WordSpecLike with Matchers with Eventually {
  import StructuredStorage._
  
  class Fixture {
    val pagedStorage = TestProbe()
    val s = TestActorRef(Props(new StructuredStorage(pagedStorage.ref)))
  }
  
  implicit object StringType extends PageType[String] {
    def fromByteString(page: ByteString): String = {
      val i = page.iterator
      val size = i.getInt
      val bytes = new Array[Byte](size)
      i.getBytes(bytes)
      new String(bytes, "UTF-8")
    }
    def toByteString(page: String): ByteString = {
      val bytes = page.getBytes("UTF-8")
      val builder = ByteString.newBuilder
      builder.putInt(bytes.length)
      builder.putBytes(bytes)
      builder.result()
    }    
  }
  
  "structured storage" should {
    "write a written page object using the page type" in new Fixture {
      s ! Write() + (PageIdx(0) -> "Hello")
      val write = pagedStorage.expectMsgType[PagedStorage.Write]
      write.pages should be (Map(PageIdx(0) -> StringType.toByteString("Hello")))
      
      pagedStorage.reply(PagedStorage.WriteCompleted(write.ctx))
      expectMsg(WriteCompleted(None))
    }
    
    "return a written object instance when read" in new Fixture {
      val value = "Hello"
      s ! Write() + (PageIdx(0) -> value)
      s ! Read(PageIdx(0))
      val msg = expectMsgType[ReadCompleted]
      msg.content should be theSameInstanceAs(value)
    }
    
    "read an unknown page from disk" in new Fixture {
      s ! Read(PageIdx(0))
      val read = pagedStorage.expectMsgType[PagedStorage.Read]
      read.page should be (PageIdx(0))
      
      pagedStorage.reply(PagedStorage.ReadCompleted(StringType.toByteString("Hello"), read.ctx))
      expectMsg(ReadCompleted("Hello", None))
    }
    
    "return the same object instance when reading same page twice" in new Fixture {
      
    }
  }
}