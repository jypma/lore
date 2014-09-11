package net.ypmania.storage.btree

import akka.util.ByteString
import scala.collection.immutable.VectorBuilder
import net.ypmania.storage.paged.PagedStorage
import net.ypmania.lore.ID
import net.ypmania.storage.paged.PageIdx
import net.ypmania.lore.BinarySearch._
import scala.collection.immutable.TreeMap
import net.ypmania.io.IO._
import BTreePage._

sealed trait BTreePage {
  def pointers: TreeMap[ID, PageIdx]
  def split(leftPageIdx: PageIdx, rightPageIdx: PageIdx)(implicit settings: BTree.Settings): (BTreePage, BTreePage, SplitResult)
  def + (tuple: (ID, PageIdx)): BTreePage
  
  def size = pointers.size
  
  def full (implicit settings: BTree.Settings) =
    size >= settings.entriesPerPage
}

case class InternalBTreePage(pointers: TreeMap[ID, PageIdx], next: PageIdx) extends BTreePage {
  // internal: 
  //   pointers._2 are locations of page with keys  < ._2
  //   next is the location of page with keys >= pointers.last._2
  
  def + (tuple: (ID, PageIdx)) = 
    copy(pointers + tuple, next)
  
  def lookup(id: ID): PageIdx = {
    val part = pointers.from(id)
    println(s"$pointers from $id is $part")
    if (part.isEmpty)
      next
    else if (part.firstKey == id) {
      val nextPart = part.drop(1)
      if (nextPart.isEmpty) next else nextPart.head._2
    } else part.head._2
  }
  
  def split(leftPageIdx: PageIdx, rightPageIdx: PageIdx)(implicit settings: BTree.Settings) = {
    val (left, rest) = pointers.splitAt(settings.order - 1)
    val key = rest.head
    val right = rest.tail
    // for internal nodes, the split key is moved to the parent
    
    (InternalBTreePage(left, key._2),
     InternalBTreePage(right, next),
     SplitResult(leftPageIdx, key._1, rightPageIdx))
  }

  def splice(info: SplitResult) = {
    if (info.leftPageIdx == next) {
      InternalBTreePage(pointers + (info.key -> info.leftPageIdx), info.rightPageIdx) 
    } else {
      val rightKey = pointers.find { case (id,page) => page == info.leftPageIdx }.get._1
      this + (info.key -> info.leftPageIdx) + (rightKey -> info.rightPageIdx)      
    }
  }
}

case class LeafBTreePage(pointers: TreeMap[ID, PageIdx], next: Option[PageIdx]) extends BTreePage {
  // leaf: 
  //   pointers._2 are actual values, next is next leaf node (or None if this is the last node)
  
  def + (tuple: (ID, PageIdx)) = 
    copy(pointers + tuple, next)
  
  def get(id: ID): Option[PageIdx] = {
    pointers.get(id)
  }

  def split(leftPageIdx: PageIdx, rightPageIdx: PageIdx)(implicit settings: BTree.Settings) = {
    val (left, rest) = pointers.splitAt(settings.order - 1)
    val key = rest.head
    val right = rest.tail
    // for leaf nodes, the split key is copied
    
    (LeafBTreePage(left, Some(rightPageIdx)),
     LeafBTreePage(rest, next),
     SplitResult(leftPageIdx, key._1, rightPageIdx))
  }
}

object BTreePage {
  case class SplitResult(leftPageIdx: PageIdx, key: ID, rightPageIdx: PageIdx) {
    def asRoot = InternalBTreePage(TreeMap(key -> leftPageIdx), rightPageIdx)
  }
  
  val empty:BTreePage = LeafBTreePage(TreeMap.empty, None)
  /*
  case class Splitting(leaf: Boolean, pointers: TreeMap[ID, PageIdx]) {
    def splitComplete(next: PageIdx) = BTreePage(leaf, pointers, Some(next))
  }
  */
  
  implicit def mkType[T <: BTreePage]: PagedStorage.PageType[T] = t.asInstanceOf[PagedStorage.PageType[T]]
  
  val t = new PagedStorage.PageType[BTreePage] {
    
    def fromByteString(bytes: ByteString) = {
      val i = bytes.iterator
      val t = i.getByte
      val leaf = (t == 0) 
      val next = if (leaf) i.getOptionPageIdx else Some(i.getPageIdx)
      val n_pointers = i.getInt
      val pointers = TreeMap.newBuilder[ID,PageIdx]
      for (n <- 0 until n_pointers) {
        val id = ID(i)
        val pagePtr = i.getPageIdx
        pointers += (id -> pagePtr)
      }
      if (leaf)
        LeafBTreePage(pointers.result, next)
      else
        InternalBTreePage(pointers.result, next.get)
    }
    
    def toByteString(page: BTreePage) = {
      val bs = ByteString.newBuilder
      page match {
        case InternalBTreePage(pointers, next) =>
          bs.putByte(1)
          bs.putPageIdx(next)

        case LeafBTreePage(pointers, next) =>
          bs.putByte(0)
          bs.putOptionPageIdx(next)
      }
      bs.putInt(page.pointers.size)
      for ((id, pagePtr) <- page.pointers) {
        id.write(bs)
        bs.putPageIdx(pagePtr)
      }
      bs.result
    }
    
    def empty = BTreePage.empty
  }
}