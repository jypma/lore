package net.ypmania.storage.paged

import DataHeader._
import akka.util.ByteStringBuilder
import net.ypmania.io.IO._
import akka.util.ByteIterator
import akka.util.ByteString

case class DataHeader(
  magic: ByteString = defaultMagic,
  pageSize: Int = defaultPageSize) {
  
  def valid = magic.sameElements(defaultMagic)
  def toByteString = {
    val bs = new ByteStringBuilder
    bs ++= magic
    bs.putInt(pageSize)
    bs.result
  }
  def offsetForPage(page: PageIdx) = DataHeader.size + (page * pageSize)
  def pageCount(fileSize: Long): PageIdx = new PageIdx(((fileSize - DataHeader.size) / pageSize).toInt)
}

object DataHeader {
  private val defaultPageSize = 64 * 1024
  private val defaultMagic = ByteString("lore-db4")
  val size = defaultMagic.length + SizeOf.Int
  def apply(i: ByteIterator) = {
    val magic = new Array[Byte](defaultMagic.length)
    i.getBytes(magic)
    new DataHeader(ByteString(magic), i.getInt)
  }
}
  
