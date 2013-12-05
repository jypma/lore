package net.ypmania.io.paged

import DataHeader._
import akka.util.ByteStringBuilder
import net.ypmania.io.IO._
import akka.util.ByteIterator

case class DataHeader(
  magic: Array[Byte] = defaultMagic,
  pageSize: Int = defaultPageSize) {
  
  def valid = magic.sameElements(defaultMagic)
  def toByteString = {
    val bs = new ByteStringBuilder
    bs.putBytes(magic)
    bs.putInt(pageSize)
    bs.result
  }
  def offsetForPage(page: PageIdx) = DataHeader.size + (page * pageSize)
  def pageCount(fileSize: Long): PageIdx = new PageIdx(((fileSize - DataHeader.size) / pageSize).toInt)
}

object DataHeader {
  private val defaultPageSize = 64 * 1024
  private val defaultMagic = "lore-db4".getBytes("UTF-8")
  val size = defaultMagic.length + SizeOf.Int
  def apply(i: ByteIterator) = {
    val magic = new Array[Byte](defaultMagic.length)
    i.getBytes(magic)
    new DataHeader(magic, i.getInt)
  }
}
  
