package net.ypmania.lore

import PagedFile._
import java.nio.ByteOrder
import akka.util.ByteString

case class MetadataPage(firstFreeList: PageIdx, branches: PageIdx, commands: PageIdx) {

}

object MetadataPage {
  implicit val byteOrder = ByteOrder.LITTLE_ENDIAN
  
  class Type extends PagedFile.PageType[MetadataPage] {
    def read(bytes: ByteString) = {
      val i = bytes.iterator
      val firstFreeList= i.getInt
      val branches = i.getInt
      val commands = i.getInt
      MetadataPage(firstFreeList, branches, commands)
    }
    
    def write(page: MetadataPage) = {
      val bs = ByteString.newBuilder
      bs.putInt(page.firstFreeList)
      bs.putInt(page.branches)
      bs.putInt(page.commands)
      bs.result
    }
  }
  
}