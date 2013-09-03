package net.ypmania.lore

import akka.util.ByteString
import java.nio.ByteOrder

case class FreeListPage(nextFreeListPage: Int, pages: Array[Int]) {

}

object FreeListPage {
  val empty = FreeListPage(-1, Array.empty)
  
  object Type extends PagedFile.PageType[FreeListPage] {
    def fromByteString(bytes: ByteString) = {
      val i = bytes.iterator
      val nextFreeListPage = i.getInt
      val n_pages = i.getInt
      val pages = Array[Int](n_pages)
      i.getInts(pages)
      FreeListPage(nextFreeListPage, pages)
    }
    
    def toByteString(page: FreeListPage) = {
      val bs = ByteString.newBuilder
      bs.putInt(page.nextFreeListPage)
      bs.putInt(page.pages.size)
      bs.putInts(page.pages)
      bs.result
    }
  }
}