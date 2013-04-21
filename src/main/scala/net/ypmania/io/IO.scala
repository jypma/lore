package net.ypmania.io

import akka.actor.IO._
import akka.util.ByteString
import net.ypmania.lore.ID

object IO {
  val takeInt = take(4) map toInt
  
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
  
  private def toInt(b:ByteString) = b.asByteBuffer.getInt
}