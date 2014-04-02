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
      println(original.pages)
      val bytes = original.toByteString
      bytes.size should be (dataHeader.pageSize * 2 + // content of two pages
                            SizeOf.Int +          // number of pages (=2)
                            SizeOf.PageIdx +      // page number of page 1
                            SizeOf.PageIdx +      // page number of page 2
                            SizeOf.MD5            // MD5
                           )
      
      val entry = JournalEntry(header, bytes)
      entry should be (original.padded)
    }
  }
}