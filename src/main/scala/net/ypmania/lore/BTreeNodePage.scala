package net.ypmania.lore

import java.nio.ByteOrder
import akka.util.ByteString
import scala.collection.immutable.VectorBuilder

case class BTreeNodePage(leaf: Boolean, firstPage: Int, pointers: Vector[(ID, Int)]) {
  // maybe split up in two subclasses, since:
     // internal pages have indeed firstpage and (ID, pageidx)*
     // leaf pages have (ID, pageidx*) and nextPage: pageidx
}

object BTreeNodePage {
  val empty = BTreeNodePage(true, -1, Vector.empty)
  
  object Type extends PagedFile.PageType[BTreeNodePage] {
    def fromByteString(bytes: ByteString) = {
      val i = bytes.iterator
      val t = i.getByte
      val leaf = (t == 0) 
      val firstPage = i.getInt
      val n_pointers = i.getInt
      val pointers = new VectorBuilder[(ID,Int)]
      for (n <- 0 until n_pointers) {
        val id = ID.getFrom(i)
        val page = i.getInt
        pointers += ((id, page))
      }
      BTreeNodePage(leaf, firstPage, pointers.result)
    }
    
    def toByteString(page: BTreeNodePage) = {
      val bs = ByteString.newBuilder
      bs.putByte(if (page.leaf) 0 else 1)
      bs.putInt(page.firstPage)
      bs.putInt(page.pointers.size)
      for (p <- page.pointers) {
        p._1.putInto(bs)
        bs.putInt(p._2)
      }
      bs.result
    }    
  }
}