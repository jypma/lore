package net.ypmania.lore.event

import net.ypmania.lore.ID
import Event._
import net.ypmania.storage.paged.PagedStorage
import akka.util.ByteString
import net.ypmania.io.IO._
import akka.util.ByteStringBuilder
import akka.util.ByteIterator

case class Event(id: ID, add: Seq[Fact], delete: Seq[Fact], storeText: Seq[(Facet,StringValue)], 
                 storeNumber: Seq[(Facet,BigDecimalValue)], clear: Seq[Facet]) {
  
  def write(bs: ByteStringBuilder) {
    id.write(bs)
    bs.putPositiveVarInt(add.size)
    for (fact <- add) fact.write(bs)
    bs.putPositiveVarInt(delete.size)
    for (fact <- delete) fact.write(bs)
    bs.putPositiveVarInt(storeText.size)
    for ((facet, value) <- storeText) {
      facet.write(bs)
      value.write(bs)
    }
    for ((facet, value) <- storeNumber) {
      facet.write(bs)
      value.write(bs)
    }
    bs.putPositiveVarInt(clear.size)
    for (facet <- clear) facet.write(bs)
  }
}

object Event {
  def apply(i: ByteIterator) = new Event(
      ID(i),
      for (_ <- 0 until i.getPositiveVarInt) yield Fact(i),
      for (_ <- 0 until i.getPositiveVarInt) yield Fact(i),
      for (_ <- 0 until i.getPositiveVarInt) yield (Facet(i), StringValue(i)),
      for (_ <- 0 until i.getPositiveVarInt) yield (Facet(i), BigDecimalValue(i)),
      for (_ <- 0 until i.getPositiveVarInt) yield Facet(i)
  )
  
  case class Fact(sub: ID, pred: ID, obj: ID) {
    def write(bs: ByteStringBuilder) {
      sub.write(bs)
      pred.write(bs)
      obj.write(bs)
    }
  }
  object Fact {
    def apply(i: ByteIterator) = new Fact(ID(i), ID(i), ID(i))
  }
  
  case class Facet(sub: ID, prop: ID) {
    def write(bs: ByteStringBuilder) {
      sub.write(bs)
      prop.write(bs)
    }
  }
  object Facet {
    def apply(i: ByteIterator) = new Facet(ID(i), ID(i))
  }
  
  trait Value {
    def asNumber: BigDecimal
    def asText: String
    def write(bs: ByteStringBuilder)
  }
  
  case class BigDecimalValue(number: BigDecimal) extends Value {
    def asNumber = number
    def asText = number.toString
    def write(bs: ByteStringBuilder) {
      val scale = number.scale
      val bigint = BigInt(number.bigDecimal.unscaledValue)
      val bytes = bigint.toByteArray
      bs.putVarInt(scale)
      bs.putPositiveVarInt(bytes.length)
      bs.putBytes(bytes)
    }
  }
  object BigDecimalValue {
    def apply(i: ByteIterator): BigDecimalValue = {
      val scale = i.getVarInt
      val length = i.getPositiveVarInt
      val bytes = new Array[Byte](length)
      i.getBytes(bytes)
      BigDecimalValue(BigDecimal(BigInt(bytes), scale))
    }
  }
  
  case class StringValue(text: String) extends Value {
    def asNumber = toNumber(text) getOrElse BigDecimal(0)
    def asText = text
    def write(bs: ByteStringBuilder) {
      bs.putPositiveVarInt(text.length())
      bs.putBytes(text.getBytes("UTF-8"))
    }
  }
  object StringValue {
    def apply(i: ByteIterator): StringValue = {
      val length = i.getPositiveVarInt
      val bytes = new Array[Byte](length)
      i.getBytes(bytes)
      StringValue(new String(bytes, "UTF-8"))
    }
  }
  
  private def toNumber(s: String) = try {
    Some(BigDecimal(s.trim()))
  } catch {
    case x:Exception => None
  }
}