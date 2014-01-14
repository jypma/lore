package net.ypmania.storage.btree

import akka.util.ByteString
import scala.collection.immutable.VectorBuilder
import net.ypmania.storage.structured.StructuredStorage
import net.ypmania.lore.ID
import net.ypmania.storage.paged.PageIdx
import net.ypmania.lore.BinarySearch._
import scala.collection.immutable.TreeMap

case class BTreePage(leaf: Boolean, pointers: TreeMap[ID, PageIdx], next: PageIdx) {
  import BTreePage._
  
  // leaf: 
  //   pointers._2 are actual values, next is next leaf node (or -1)
  // internal: 
  //   pointers._2 are locations of page with keys  < ._2
  //   next is the location of page with keys >= pointers.last._2
  
  def internal = !leaf
  
  def + (tuple: (ID, PageIdx)) = 
    copy(leaf, pointers + tuple, next)
  
  def size = pointers.size
  
  def full (implicit settings: BTree.Settings) =
    size >= settings.entriesPerPage
    
  def get(id: ID): Option[PageIdx] = {
    if (internal) throw new IllegalStateException("Accessing internal as leaf")
    pointers.get(id)
  }

  def lookup(id: ID): PageIdx = {
    if (leaf) throw new IllegalStateException("Accessing leaf as internal")
    val elem = pointers.from(id)
    if (elem.isEmpty)
      next
      else
        elem.head._2
  }
  
  def split(implicit settings: BTree.Settings): (BTreePage, ID, BTreePage) = {
    val (left, rest) = pointers.splitAt(settings.order - 1)
    val key = rest.head
    val right = rest.tail
    if (leaf) {
      // for leaf nodes, the split key is copied
      (BTreePage(leaf, left, key._2),
       key._1,
       BTreePage(leaf, rest, next))
    } else {
      // for internal nodes, the split key is moved to the parent
      (BTreePage(leaf, left, key._2),
       key._1,
       BTreePage(leaf, right, next))
    }
  }
}

object BTreePage {
  val empty = BTreePage(true, TreeMap.empty, PageIdx(-1))
  
  case class Splitting(leaf: Boolean, pointers: TreeMap[ID, PageIdx]) {
    def splitComplete(next: PageIdx) = BTreePage(leaf, pointers, next)
  }
  
  implicit object Type extends StructuredStorage.PageType[BTreePage] {
    def fromByteString(bytes: ByteString) = {
      val i = bytes.iterator
      val t = i.getByte
      val leaf = (t == 0) 
      val next = PageIdx.get(i)
      val n_pointers = i.getInt
      val pointers = TreeMap.newBuilder[ID,PageIdx]
      for (n <- 0 until n_pointers) {
        val id = ID.getFrom(i)
        val pagePtr = PageIdx.get(i)
        pointers += (id -> pagePtr)
      }
      BTreePage(leaf, pointers.result, next)
    }
    
    def toByteString(page: BTreePage) = {
      val bs = ByteString.newBuilder
      bs.putByte(if (page.leaf) 0 else 1)
      page.next.put(bs)
      bs.putInt(page.pointers.size)
      for ((id, pagePtr) <- page.pointers) {
        id.putInto(bs)
        pagePtr.put(bs)
      }
      bs.result
    }    
  }
}