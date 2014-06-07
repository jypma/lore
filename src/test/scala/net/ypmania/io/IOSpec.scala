package net.ypmania.io

import org.scalatest.Matchers
import org.scalatest.WordSpec
import akka.util.ByteString

class IOSpec extends WordSpec with Matchers {
  import IO._

  def asPositiveVarint(i: Integer) = {
      val bs = ByteString.newBuilder
      bs.putPositiveVarint(i)
      bs.result()    
  }
  
  "putting and getting positive varints" should {
    "encode the right number in various edge cases" in {
      asPositiveVarint(0) should be (ByteString(0))
      asPositiveVarint(1) should be (ByteString(1))
      asPositiveVarint(127) should be (ByteString(127))
      asPositiveVarint(128) should be (ByteString(0x80, 1))
      asPositiveVarint(129) should be (ByteString(0x81, 1))
    }
  }
}