package net.ypmania.storage.paged

import akka.util.ByteString
import java.security.MessageDigest
import akka.util.ByteStringBuilder
import net.ypmania.io.IO._
import scala.collection.immutable.TreeMap
import akka.util.ByteIterator

case class JournalEntryIndex (header: JournalHeader, pageLengths: Map[PageIdx,Int], md5: ByteString) {
  private var length: Option[Int] = None 
  private var byteOffsets: Option[Map[PageIdx,Int]] = None
  
  lazy val toByteString = {
    val offsets = Map.newBuilder[PageIdx, Int]
    var offset = 0
    val bs = new ByteStringBuilder
    bs.putPositiveVarInt(pageLengths.size)
    for ((page, size) <- pageLengths) {
      offsets += (page -> offset)
      offset += size
      bs.putPageIdx(page)
      bs.putPositiveVarInt(size)
    }
    bs ++= md5
    length = Some(bs.length)
    byteOffsets = Some(offsets.result.mapValues(_ + length.get))
    bs.result
  }
  
  def indexSize:Int = {
    if (length.isEmpty) toByteString
    length.get
  }
  
  def entrySize = indexSize + pageLengths.values.sum
  
  def byteOffset(page: PageIdx) = {
    if (byteOffsets.isEmpty) toByteString
    (byteOffsets.get)(page)
  }
}

case object JournalEntryIndex {
  def apply(header: JournalHeader, i: ByteIterator) = {
    val before = i.len
    val pages = Map.newBuilder[PageIdx,Int]
    for (idx <- 0 until i.getPositiveVarInt) {
      pages += (i.getPageIdx -> i.getPositiveVarInt)
    }
    val readMd5 = new Array[Byte](SizeOf.MD5)
    i.getBytes(readMd5)
    val length = before - i.len
    val result = new JournalEntryIndex(header, pages.result, ByteString(readMd5))
    result.length = Some(length)
    result
  }
}

case class JournalEntry private (index: JournalEntryIndex, pages: Map[PageIdx, ByteString]) {
  
  lazy val toByteString = {
    val bs = new ByteStringBuilder
    bs ++= index.toByteString 
    for (content ← pages.values) {
      bs ++= content
    }
    bs.result
  }
}

object JournalEntry {
  def apply(header: JournalHeader, i: ByteIterator) = {
    val index = JournalEntryIndex(header, i)
    val pages = Map.newBuilder[PageIdx, ByteString]
    for ((page, length) <- index.pageLengths) {
      // Waiting on https://www.assembla.com/spaces/ddEDvgVAKr3QrUeJe5aVNr/tickets/3516
      pages += (page -> i.clone.take(length).toByteString)
      i.drop(length)
    }

    val pageMap = pages.result()
    val expectedMd5 = md5(header, pageMap)
    if (expectedMd5 != index.md5)
      throw new Exception (s"Wrong MD5 in journal entry: expected ${expectedMd5}, got ${index.md5}") // TODO ignore journal from here
    
    new JournalEntry(index, pageMap)
  }
  
  def apply(header: JournalHeader, pages: Map[PageIdx, ByteString]) = {
    val lengths = pages.mapValues(_.length)
    new JournalEntry(new JournalEntryIndex(header, lengths, md5(header, pages)), pages)
  }
  
  private def md5(header: JournalHeader, pages: Map[PageIdx, ByteString]) = {
    val md = MessageDigest.getInstance("MD5")
    for ((pageIdx, content) ← TreeMap.empty[PageIdx,ByteString] ++ pages) {
      val b = new ByteStringBuilder()
      b.putPageIdx(pageIdx)
      md.update(b.result.asByteBuffer)
      
      md.update(content.zeroPad(header.pageSize).asByteBuffer)
    }
    ByteString(md.digest())
  }
}
  
