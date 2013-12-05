package net.ypmania.io.paged

import akka.util.ByteString
import java.security.MessageDigest
import akka.util.ByteStringBuilder

import net.ypmania.io.IO._

case class JournalEntry private (
    header: JournalHeader,
    pages: Map[PageIdx, ByteString], 
    md5: ByteString) {
  def toByteString = {
    val bs = new ByteStringBuilder
    bs ++= md5
    bs.putInt(pages.size)
    for (pageIdx ← pages.keys) {
      pageIdx.put(bs)
    }
    for (content ← pages.values) {
      bs ++= content
      if (content.size < header.pageSize) {
        // Fill with zeroes
        bs ++= header.emptyPage.take(header.pageSize - content.size)
      }
    }
    bs.result
  }
}
object JournalEntry {
  def apply(header: JournalHeader, pages: Map[PageIdx, ByteString]) = {
    val md = MessageDigest.getInstance("MD5")
    for (pageIdx ← pages.keys) {
      val b = new ByteStringBuilder()
      pageIdx.put(b)
      md.update(b.result.asByteBuffer)
    }
    for (content ← pages.values) {
      md.update(content.asByteBuffer)
      if (content.size < header.pageSize) {
        // Fill with zeroes
        md.update(header.emptyPageArray, 0, header.pageSize - content.size)
      }
    }
    new JournalEntry(header, pages, ByteString(md.digest()))
  }
}
  
