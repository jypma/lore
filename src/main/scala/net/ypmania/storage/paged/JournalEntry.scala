package net.ypmania.storage.paged

import akka.util.ByteString
import java.security.MessageDigest
import akka.util.ByteStringBuilder
import net.ypmania.io.IO._
import scala.collection.immutable.TreeMap

case class JournalEntry private (
    header: JournalHeader,
    pages: Map[PageIdx, ByteString], 
    md5: ByteString) {
  def toByteString = {
    val bs = new ByteStringBuilder
    bs.putInt(pages.size)
    for (pageIdx ← pages.keys) {
      pageIdx.put(bs)
    }
    for (content ← pages.values) {
      bs ++= content.zeroPad(header.pageSize)
    }
    bs ++= md5
    bs.result
  }
  
  def padded = copy(pages = pages.mapValues(_.zeroPad(header.pageSize)))
}

object JournalEntry {
  def apply(header: JournalHeader, bytes: ByteString) = {
    val i = bytes.iterator
    val pageCount = i.getInt
    val pageIdxs = for (p <- 0 until pageCount) yield PageIdx.get(i)
    val pages = Map.newBuilder[PageIdx, ByteString]
    
    var pos = SizeOf.Int + (pageCount * SizeOf.PageIdx)
    for (pageIdx <- pageIdxs) {
      pages += (pageIdx -> bytes.slice(pos, pos + header.pageSize))
      pos = pos + header.pageSize
      i.drop(header.pageSize)
    }
    val readMd5 = new Array[Byte](SizeOf.MD5)
    i.getBytes(readMd5)
    val pageMap = pages.result()
    val expectedMd5 = md5(header, pageMap)
    if (expectedMd5 != ByteString(readMd5))
      throw new Exception (s"Wrong MD5 in journal entry: expected ${expectedMd5}, got ${ByteString(readMd5)}") // TODO ignore journal from here
    
    new JournalEntry(header, pageMap, expectedMd5)
  }
  
  def apply(header: JournalHeader, pages: Map[PageIdx, ByteString]) = {
    new JournalEntry(header, pages, md5(header, pages))
  }
  
  private def md5(header: JournalHeader, pages: Map[PageIdx, ByteString]) = {
    val md = MessageDigest.getInstance("MD5")
    for ((pageIdx, content) ← TreeMap.empty[PageIdx,ByteString] ++ pages) {
      val b = new ByteStringBuilder()
      pageIdx.put(b)
      md.update(b.result.asByteBuffer)
      
      md.update(content.zeroPad(header.pageSize).asByteBuffer)
    }
    ByteString(md.digest())
  }
}
  
