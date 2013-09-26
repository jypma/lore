package net.ypmania.lore

import java.nio.ByteOrder
import akka.util.ByteString

case class MetadataPage(firstFreeList: PageIdx, branches: PageIdx, commands: PageIdx) {

}

object MetadataPage {
  val emptyDb = MetadataPage(PageIdx(1), PageIdx(2), PageIdx(3))
  
  object Type extends StructuredStorage.PageType[MetadataPage] {
    def fromByteString(bytes: ByteString) = {
      val i = bytes.iterator
      val firstFreeList= PageIdx.get(i)
      val branches = PageIdx.get(i)
      val commands = PageIdx.get(i)
      MetadataPage(firstFreeList, branches, commands)
    }
    
    def toByteString(page: MetadataPage) = {
      val bs = ByteString.newBuilder
      page.firstFreeList.put(bs)
      page.branches.put(bs)
      page.commands.put(bs)
      bs.result
    }
  }
  
}