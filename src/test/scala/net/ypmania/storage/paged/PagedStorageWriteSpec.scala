package net.ypmania.storage.paged

import org.scalatest.Matchers
import org.scalatest.WordSpec
import net.ypmania.storage.paged.PagedStorage.PageType
import akka.util.ByteString
import org.scalamock.scalatest.MockFactory
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.testkit.TestProbe
import net.ypmania.storage.paged.PagedStorage.Write

class PagedStorageWriteSpec extends WordSpec with Matchers {
  trait Fixture {    
    implicit val byteStringPageType = new PageType[ByteString] {
      def fromByteString(page: ByteString) = page
          def toByteString(page: ByteString) = page
    }
    
    val system = ActorSystem("Test")
    def createActorRef() = TestProbe()(system).ref
  }
  
  "a Write to a PagedStorage" should {
    val content = ByteString("Hello, world!")
    
    "merge writes from several actors when no duplicate page writes are present" in new Fixture {
      val author1 = createActorRef()
      val author2 = createActorRef()
      val w = Write(PageIdx(0) -> content)(implicitly, author1) .+ (PageIdx(1) -> content)(implicitly, author2)
      w.pages.size should be (2)
    }
    
    "merge writes when one actor wrote the same page several times, taking the latest write" in new Fixture{
      implicit val author = createActorRef()
      val newContent = ByteString("Goodbye, world!")
      val w = Write(PageIdx(0) -> content) + (PageIdx(0) -> newContent)
      w.pages.size should be (1)
      w.pages(PageIdx(0)).content should be (newContent)
      w.pages(PageIdx(0)).author should be (author)
    }
    
    "blow up trying to merge the same page with different authors" in new Fixture {
      val w = {
        implicit val author1 = createActorRef()
        Write(PageIdx(0) -> content)
      }
      
      intercept[IllegalArgumentException] {
        implicit val author2 = createActorRef()
        w + (PageIdx(0) -> content)
      }
    }
    
  }
}