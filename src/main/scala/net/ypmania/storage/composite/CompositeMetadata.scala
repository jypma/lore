package net.ypmania.storage.composite

import net.ypmania.storage.paged.PageIdx
import net.ypmania.storage.paged.PagedStorage.PageType
import akka.util.ByteString
import net.ypmania.io.IO._

case class CompositeMetadata(pageFreeSpace: Map[PageIdx,Int]) {
  def pageWithSpace(size: Int): Option[PageIdx] = pageFreeSpace find { case (page,space) => space >= size } map (_._1)
  def alloc(page: PageIdx, bytesUsed: Int, pageSize: Int) = {
    val freeSpace = pageFreeSpace.get(page) map (_ - bytesUsed) getOrElse (pageSize - bytesUsed)
    assert (freeSpace >= 0)
    CompositeMetadata(pageFreeSpace + (page -> freeSpace))
  }
}

object CompositeMetadata {
  implicit object Type extends PageType[CompositeMetadata] {
    def fromByteString(page: ByteString) = {
      val i = page.iterator
      val count = i.getPositiveVarInt
      val pageFreeSpace = Map.newBuilder[PageIdx,Int]
      for (idx <- 0 until count) {
        pageFreeSpace += i.getPageIdx -> i.getPositiveVarInt
      }
      CompositeMetadata(pageFreeSpace.result)
    }
    
    def toByteString(page: CompositeMetadata) = {
      val bs = ByteString.newBuilder
      bs.putPositiveVarInt(page.pageFreeSpace.size)
      for ((pageIdx, space) <- page.pageFreeSpace) {
        bs.putPageIdx(pageIdx)
        bs.putPositiveVarInt(space)
      }
      bs.result
    }
    
    def empty = CompositeMetadata(Map.empty)
  }
}
