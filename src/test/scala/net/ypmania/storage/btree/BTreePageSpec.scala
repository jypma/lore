package net.ypmania.storage.btree

import org.scalatest.WordSpec
import org.scalatest.Matchers
import net.ypmania.lore.BaseID
import scala.collection.immutable.TreeMap
import net.ypmania.storage.paged.PageIdx
import net.ypmania.storage.btree.BTreePage.SplitResult

class BTreePageSpec extends WordSpec with Matchers {
  implicit val settings = BTree.Settings(order = 2)
  
  "A BTreePage" should {
    "serialize successfully" in {
      val page = LeafBTreePage(TreeMap(BaseID(1,1,2) -> PageIdx(102), BaseID(1,1,3) -> PageIdx(103)),None)
      val loaded = BTreePage.t.fromByteString(BTreePage.t.toByteString(page))
      
      loaded should be (page)
    }
    
    "more cases" in {
      val page = LeafBTreePage(TreeMap(BaseID(1,1,481172799) -> PageIdx(155)),Some(PageIdx(2))) 
      val loaded = BTreePage.t.fromByteString(BTreePage.t.toByteString(page))
      
      loaded should be (page)      
    }
  }
  
  "A leaf, full BTreePage" should {
    val pageIdx = PageIdx(1)
    val nextLeafPage = PageIdx(2)
    val page = LeafBTreePage(TreeMap(BaseID(1,1,1) -> PageIdx(101), BaseID(1,1,2) -> PageIdx(102), BaseID(1,1,3) -> PageIdx(103)), Some(nextLeafPage))
    
    "split into two leaf pages, copying the key to the parent" in {
      val rightPage = PageIdx(3)
      val (left, right, info) = page.split(pageIdx, rightPage)
      left should be (LeafBTreePage(TreeMap(BaseID(1,1,1) -> PageIdx(101)), Some(rightPage)))
      info should be (SplitResult(pageIdx, BaseID(1,1,2), rightPage))
      right should be (LeafBTreePage(TreeMap(BaseID(1,1,2) -> PageIdx(102), BaseID(1,1,3) -> PageIdx(103)), Some(nextLeafPage)))
    }
  }
  
  "An internal, full BTreePage" should {
    val pageWithLargerIDs = PageIdx(4)
    val pageIdx = PageIdx(6)
    val page = InternalBTreePage(TreeMap(BaseID(1,1,10) -> PageIdx(1), BaseID(1,1,20) -> PageIdx(2), BaseID(1,1,30) -> PageIdx(3)), pageWithLargerIDs)
    
    "split into two internal nodes, moving the split key to the parent" in {
      val rightPage = PageIdx(5)
      val (left, right, info) = page.split(pageIdx, rightPage)
      left should be (InternalBTreePage(TreeMap(BaseID(1,1,10) -> PageIdx(1)), PageIdx(2)))
      info should be (SplitResult(pageIdx, BaseID(1,1,20), rightPage))
      right should be (InternalBTreePage(TreeMap(BaseID(1,1,30) -> PageIdx(3)), pageWithLargerIDs))      
    }
  }
  
  "An internal BTreePage" should {
    val page = InternalBTreePage(TreeMap(
          BaseID(1,1,10) -> PageIdx(1), BaseID(1,1,30) -> PageIdx(3)),
          PageIdx(4))
          
    "look up nodes correctly" in {
      // Lookups < key should go to the value, >= key should go to the next value.
      page.lookup(BaseID(1,1,9)) should be (PageIdx(1))
      page.lookup(BaseID(1,1,10)) should be (PageIdx(3))
      page.lookup(BaseID(1,1,11)) should be (PageIdx(3))
      page.lookup(BaseID(1,1,30)) should be (PageIdx(4))
      page.lookup(BaseID(1,1,31)) should be (PageIdx(4))
    }
          
    "place a new node at start correctly" in {
      val updated = page.splice(BTreePage.SplitResult(PageIdx(1), BaseID(1,1,5), PageIdx(2)))
      updated.pointers should be (Map(BaseID(1,1,5) -> PageIdx(1), BaseID(1,1,10) -> PageIdx(2), BaseID(1,1,30) -> PageIdx(3)))
      updated.next should be (PageIdx(4))
    }
    
    "place a new node in the middle correctly" in {
      val updated = page.splice(BTreePage.SplitResult(PageIdx(3), BaseID(1,1,20), PageIdx(5)))
      updated.pointers should be (Map(BaseID(1,1,10) -> PageIdx(1), BaseID(1,1,20) -> PageIdx(3), BaseID(1,1,30) -> PageIdx(5)))
      updated.next should be (PageIdx(4))
    }
    
    "place a new node at the end correctly" in {
      val updated = page.splice(BTreePage.SplitResult(PageIdx(4), BaseID(1,1,40), PageIdx(5)))
      updated.pointers should be (Map(BaseID(1,1,10) -> PageIdx(1), BaseID(1,1,30) -> PageIdx(3), BaseID(1,1,40) -> PageIdx(4)))
      updated.next should be (PageIdx(5))
      
    }
    
  }
}