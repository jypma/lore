package net.ypmania.storage.paged

class PageIdx (val idx: Int) extends AnyVal with Ordered[PageIdx] {
  def * (bytesPerPage: Int) = idx * bytesPerPage
  def + (that: Int) = new PageIdx(idx + that)
  def max (that: PageIdx) = new PageIdx (idx.max (that.idx))
  def min (that: PageIdx) = new PageIdx (idx.min (that.idx))
  
  def toInt = idx
  
  def compare(that: PageIdx) = idx - that.idx
  
  override def toString = s"PageIdx(${idx})"
}

object PageIdx {
  def apply(value: Int) = {
    require (value >= 0, s"Cannot have a negative PageIdx of $value")
    new PageIdx(value)
  }
}