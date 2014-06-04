package net.ypmania.io

import org.scalatest.Matchers
import org.scalatest.WordSpec
import akka.util.ByteString

class IOSpec extends WordSpec with Matchers {
  import IO._
/*  
  def asVarInt(i: Integer) = {
      val bs = ByteString.newBuilder
      bs.putVarInt(i)
      bs.result()    
  }
  
  def asVarIntUnsigned(i: Integer) = {
      val bs = ByteString.newBuilder
      bs.putVarIntUnsigned(i)
      bs.result()    
  }
  
  "putting and getting unsigned varints" should {
    "return the same number in various edge cases" in {
      asVarIntUnsigned(0) should be (ByteString(0))
      asVarIntUnsigned(1) should be (ByteString(1))
      asVarIntUnsigned(127) should be (ByteString(127))
      asVarIntUnsigned(128) should be (ByteString(0x80, 1))
      asVarIntUnsigned(129) should be (ByteString(0x81, 1))
    }
  }*/
}