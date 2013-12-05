package net.ypmania.io.paged

import net.ypmania.io.IO._
import JournalHeader._
import akka.util.ByteIterator
import akka.util.ByteStringBuilder
import akka.util.ByteString

case class JournalHeader private (
    magic: Array[Byte],
    pageSize: Int) {
  def valid = magic.sameElements(JournalHeader.defaultMagic)
  def toByteString = {
    val bs = new ByteStringBuilder
    bs.putBytes(magic)
    bs.putInt(pageSize)
    bs.result
  }
  
  lazy val emptyPageArray = new Array[Byte](pageSize)
  lazy val emptyPage = ByteString(emptyPageArray)
}

object JournalHeader {
  private val defaultMagic = "lore-jr4".getBytes("UTF-8")      
  val size = defaultMagic.length + SizeOf.Int
      
  def apply (dataHeader: DataHeader) = 
    new JournalHeader(defaultMagic, dataHeader.pageSize)
  
  def apply(i: ByteIterator) = {
    val magic = new Array[Byte](defaultMagic.length)
    i.getBytes(magic)
    new JournalHeader(magic, i.getInt)
  }
  
}  
