package net.ypmania.lore

import java.security.MessageDigest
import java.net.NetworkInterface
import scala.collection.JavaConversions._
import net.ypmania.io.IO
import akka.util.ByteStringBuilder
import java.nio.ByteOrder
import akka.util.ByteIterator

case class ID (l1: Long, l2: Long) extends Ordered[ID] {
  import ID._

  def compare(that: ID) = {
    def d1 = this.l1 - that.l1
    intBounds(
      if (d1 == 0)
        this.l2 - that.l2
      else
        d1)
  }
  
  def idType = l2 & 0xFFFF
  def isValueProperty = idType == 4
  def isTextProperty = idType == 5
}

object ID {
  def forBranch = forType(1)
  def forChange = forType(2)
  def forMerge = forType(3)
  def forValueProperty = forType(4)
  def forTextProperty = forType(5)
  
  private def forType(t:Int) = ID(time, node | t) 
  
  private def intBounds(l: Long) =
    if (l.isValidInt) l.toInt else if (l < 0) Int.MinValue else Int.MaxValue
    
  private val node = {
    val hash = MessageDigest.getInstance("MD5")
    NetworkInterface.getNetworkInterfaces().foreach { iface =>
      if (iface.getHardwareAddress != null) hash.update(iface.getHardwareAddress)
    }
    val long = IO.toLong (hash.digest, 0)
    long & 0xFFFFFFFFFFFFl << 16
  }
  
  private var lastMs:Long = 0
  private var msSeq:Int = 0
  private def time = {
    val ms = System.currentTimeMillis - 1356998400000l // 1/1/2013 0:00 GMT
    val seq = synchronized {
      if (ms == lastMs) {
        msSeq += 1
      } else {
        msSeq = 0
      }
      lastMs = ms
      msSeq
    }
    if (seq > 0xFFFF) throw new Exception("ID generator overflow: too many IDs per ms")
    if (ms > 0xFFFFFFFFFFFFl) throw new Exception("ID generator overflow: out of time")
    ms << 16 | seq
  }
}