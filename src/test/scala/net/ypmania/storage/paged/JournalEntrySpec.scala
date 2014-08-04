package net.ypmania.storage.paged

import org.scalatest.Matchers
import org.scalatest.WordSpec
import akka.util.ByteString
import net.ypmania.io.IO.SizeOf

class JournalEntrySpec extends WordSpec with Matchers {
  class Fixture {
    val dataHeader = DataHeader(pageSize = 64)
    val header = JournalHeader(dataHeader)
  }
  
  "a JournalEntry" should {
    "survive serialization to ByteString" in new Fixture {
      val original = JournalEntry(header, 
          Map(PageIdx(1) -> ByteString("Hello, world"),
              PageIdx(2) -> ByteString("With fine people")))
      val bytes = original.toByteString
      
      val entry = JournalEntry(header, bytes.iterator)
      entry should be (original)
    }
  }
}