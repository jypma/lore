package net.ypmania.storage.paged

import java.nio.ByteOrder
import akka.util.ByteIterator
import akka.util.ByteStringBuilder

class PageIdx (val idx: Int) extends AnyVal with Ordered[PageIdx] {
  def * (bytesPerPage: Int) = idx * bytesPerPage
  def + (that: Int) = new PageIdx(idx + that)
  def max (that: PageIdx) = new PageIdx (idx.max (that.idx))
  def min (that: PageIdx) = new PageIdx (idx.min (that.idx))
  
  def put (bs: ByteStringBuilder)(implicit byteOrder: ByteOrder) {
    bs.putInt(idx)
  }
  
  def toInt = idx
  
  def compare(that: PageIdx) = idx - that.idx
  
  override def toString = s"PageIdx(${idx})"
}

object PageIdx {
  def apply(value: Int) = new PageIdx(value)
  def get(i: ByteIterator)(implicit byteOrder: ByteOrder) = new PageIdx(i.getInt)
}