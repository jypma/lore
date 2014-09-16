package net.ypmania.lore

import java.security.MessageDigest
import java.net.NetworkInterface
import scala.collection.JavaConversions._
import akka.util.ByteStringBuilder
import java.nio.ByteOrder
import akka.util.ByteIterator
import net.ypmania.io.IO._

sealed trait ID extends Ordered[ID] {
  def write(bs: ByteStringBuilder): Unit
  
  def derive(derivative: ID) = new DerivedID(this, derivative)
}

case class BaseID (node: Short, time: Int, seq: Int) extends ID {
  require(node >= 0)
  require(time >= 0)
  require(seq >= 0)
  
  def compare(other: ID) = other match {
    case that: BaseID =>
      val dTime = this.time - that.time
      if (dTime != 0) dTime else {
        val dSeq = this.seq - that.seq
        if (dSeq != 0) dSeq else {
          this.node - that.node
        }
      }
    case derivedId => Int.MinValue  // BaseID is always < DerivedID
  }
  
  def write(bs: ByteStringBuilder): Unit = {
    bs.putPositiveVarInt(node)    
    bs.putPositiveVarInt(time)
    bs.putPositiveVarInt(seq)
  }
}

case class DerivedID(original: ID, derivative: ID) extends ID {
  def compare(other: ID) = other match {
    case that: DerivedID => 
      val dOriginal = this.original.compare(that.original)
      if (dOriginal != 0) dOriginal else {
        this.derivative.compare(that.derivative)
      }
    case baseId => Int.MaxValue // DerivedID is always > BaseID
  }
  def write(bs: ByteStringBuilder): Unit = {
    bs.putByte(0)
    original.write(bs)
    derivative.write(bs)
  }
}

object ID {
  def apply(i: ByteIterator): ID = {
    val node = i.getPositiveVarInt.toShort
    if (node == 0) {
      new DerivedID(ID(i), ID(i))
    } else {
      new BaseID(node, i.getPositiveVarInt, i.getPositiveVarInt)
    }
  }
}