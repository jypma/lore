package net.ypmania.lore.db

import akka.util.ByteString
import net.ypmania.storage.paged.PageIdx
import net.ypmania.storage.paged.PagedStorage
import net.ypmania.io.IO._

/**
 * @param branchesIndex PageIdx of BTree index of BranchPage
 * @param events PageIdx of last non-empty EventsPage
 */
case class MetadataPage(firstFreeList: PageIdx, branchesIndex: PageIdx, events: PageIdx) {

}

object MetadataPage {
  val emptyDb = MetadataPage(PageIdx(1), PageIdx(2), PageIdx(3))
  
  implicit object Type extends PagedStorage.PageType[MetadataPage] {
    def fromByteString(bytes: ByteString) = {
      val i = bytes.iterator
      val firstFreeList= i.getPageIdx
      val branches = i.getPageIdx
      val events = i.getPageIdx
      MetadataPage(firstFreeList, branches, events)
    }
    
    def toByteString(page: MetadataPage) = {
      val bs = ByteString.newBuilder
      bs.putPageIdx(page.firstFreeList)
      bs.putPageIdx(page.branchesIndex)
      bs.putPageIdx(page.events)
      bs.result
    }
    
    def empty = MetadataPage.emptyDb
  }
  
}