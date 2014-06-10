package net.ypmania.io

import org.scalatest.Matchers
import org.scalatest.WordSpec
import akka.util.ByteString

class IOSpec extends WordSpec with Matchers {
  import IO._

  def asVarint(i: Integer) = {
    val bs = ByteString.newBuilder
    bs.putVarInt(i)
    bs.result()    
  }
  
  def fromVarint(bs: ByteString) = {
    bs.iterator.getVarInt
  }
  
  def asPositiveVarint(i: Integer) = {
    val bs = ByteString.newBuilder
    bs.putPositiveVarInt(i)
    bs.result()    
  }
  
  def fromPositiveVarint(bs: ByteString) = {
    bs.iterator.getPositiveVarInt
  }
  
  "putting and getting signed varints" should {
    "encode the right number in various edge cases" in {
      asVarint(0) should be (ByteString(0))
      asVarint(-1) should be (ByteString(1))
      asVarint(1) should be (ByteString(2))
      asVarint(-2) should be (ByteString(3))
      asVarint(63) should be (ByteString(126))
      asVarint(-64) should be (ByteString(127))
      asVarint(64) should be (ByteString(0x80, 1))
      asVarint(-65) should be (ByteString(0x81, 1))
    }
    
    "decode the right number in various edge cases" in {
      fromVarint(ByteString(0)) should be (0)
      fromVarint(ByteString(1)) should be (-1)
      fromVarint(ByteString(2)) should be (1)
      fromVarint(ByteString(3)) should be (-2)
      fromVarint(ByteString(126)) should be (63)
      fromVarint(ByteString(127)) should be (-64)
      fromVarint(ByteString(0x80, 1)) should be (64)
      fromVarint(ByteString(0x81, 1)) should be (-65)      
    }
    
    "blow up when decoding too long numbers" in {
      intercept[IllegalArgumentException] { ByteString(0x80, 0x80, 0x80, 0x80, 0x80, 1).iterator.getPositiveVarInt }
      intercept[IllegalArgumentException] { ByteString(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 1).iterator.getPositiveVarLong }      
    }
  }  
  "putting and getting positive varints" should {
    "encode the right number in various edge cases" in {
      asPositiveVarint(0) should be (ByteString(0))
      asPositiveVarint(1) should be (ByteString(1))
      asPositiveVarint(127) should be (ByteString(127))
      asPositiveVarint(128) should be (ByteString(0x80, 1))
      asPositiveVarint(129) should be (ByteString(0x81, 1))
    }
    
    "decode the right number in various edge cases" in {
      fromPositiveVarint(ByteString(0)) should be (0)
      fromPositiveVarint(ByteString(1)) should be (1)
      fromPositiveVarint(ByteString(127)) should be (127)
      fromPositiveVarint(ByteString(0x80, 1)) should be (128)
      fromPositiveVarint(ByteString(0x81, 1)) should be (129)
    }
  }
}