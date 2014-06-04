package net.ypmania.io

import akka.util.ByteString
import net.ypmania.lore.ID
import java.nio.ByteOrder
import scala.annotation.tailrec
import akka.util.ByteStringBuilder
import akka.util.ByteIterator
import net.ypmania.storage.paged.PageIdx

object IO {
  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN
  
  def toID(buf: Array[Byte], ofs:Int) =
    ID(toLong(buf, ofs), toLong(buf, ofs + 8))
  
  def toLong(buf: Array[Byte], ofs: Int) = 
    ((buf(ofs  ) & 0xFF).asInstanceOf[Long] << 56) |
    ((buf(ofs+1) & 0xFF).asInstanceOf[Long] << 48) |
    ((buf(ofs+2) & 0xFF).asInstanceOf[Long] << 40) |
    ((buf(ofs+3) & 0xFF).asInstanceOf[Long] << 32) |
    ((buf(ofs+4) & 0xFF).asInstanceOf[Long] << 24) |
    ((buf(ofs+5) & 0xFF).asInstanceOf[Long] << 16) |
    ((buf(ofs+6) & 0xFF).asInstanceOf[Long] << 8) |
     (buf(ofs+7) & 0xFF).asInstanceOf[Long]
  
  implicit class ByteStringOps(val bs: ByteString) {
    private lazy val zeroes = ByteString(new Array[Byte](128 * 1024))
    
    @tailrec 
    final def zeroPad(length: Integer): ByteString = {
      if (bs.length >= length)
        bs
      else {
        val needed = length - bs.length 
        if (needed <= zeroes.length) 
          bs ++ zeroes.take(needed)
        else
          (bs ++ zeroes).zeroPad(needed - zeroes.length)  
      }
    }
  }
  
  implicit class ByteStringBuilderOps(val bs: ByteStringBuilder) {
    def putID(id:ID) {
      bs.putLong(id.l1)
      bs.putLong(id.l2)      
    }
    
    def putPageIdx(p:PageIdx) {
      bs.putInt(p.toInt)
    }
  }
  
  implicit class ByteIteratorOps(val i: ByteIterator) {
    def getID: ID = {
      val l1 = i.getLong
      val l2 = i.getLong
      new ID(l1, l2)   
    }
    def getPageIdx = PageIdx(i.getInt)
  }
  
  private def toInt(b:ByteString) = b.asByteBuffer.getInt
  
  object SizeOf {
    val Int = 4
    val Long = 8
    val MD5 = 16
    val PageIdx = Int
  }
}