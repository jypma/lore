package net.ypmania.lore.db

import akka.util.ByteString
import net.ypmania.storage.paged.PageIdx
import net.ypmania.storage.paged.PagedStorage
import net.ypmania.io.IO._
import net.ypmania.lore.BaseID

/**
 * @param branchesIndex PageIdx of BTree index of BranchPage
 * @param events PageIdx of last non-empty EventsPage
 */
case class MetadataPage(firstFreeList: PageIdx, branchesIndex: PageIdx, events: PageIdx, creation: Long, thisNode: Short, nextNode: Short) {
  import MetadataPage._
  private var lastTime:Long = 0
  private var lastSeq:Int = 0
  
  require(thisNode > 0)
  require(nextNode > thisNode)
  
  def createID() = {
    val time = (currentTimeSeconds() - creation).toInt
    val seq = synchronized {
      if (time == lastTime) {
        lastSeq += 1
      } else {
        lastSeq = 0
        lastTime = time
      }
      lastSeq
    }
    BaseID(thisNode, time, seq)
  }

}

object MetadataPage {
  private def currentTimeSeconds() = System.currentTimeMillis() / 1000 
  val emptyDb = MetadataPage(PageIdx(1), PageIdx(2), PageIdx(3), currentTimeSeconds(), 1, 2)
  
  implicit object Type extends PagedStorage.PageType[MetadataPage] {
    def fromByteString(bytes: ByteString) = {
      val i = bytes.iterator
      val firstFreeList= i.getPageIdx
      val branches = i.getPageIdx
      val events = i.getPageIdx
      val creation = i.getLong
      val thisNode = i.getShort
      val nextNode = i.getShort
      MetadataPage(firstFreeList, branches, events, creation, thisNode, nextNode)
    }
    
    def toByteString(page: MetadataPage) = {
      val bs = ByteString.newBuilder
      bs.putPageIdx(page.firstFreeList)
      bs.putPageIdx(page.branchesIndex)
      bs.putPageIdx(page.events)
      bs.putLong(page.creation)
      bs.putShort(page.thisNode)
      bs.putShort(page.nextNode)
      bs.result
    }
    
    def empty = MetadataPage.emptyDb
  }
  
}