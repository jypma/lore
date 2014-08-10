package net.ypmania.storage.composite

import akka.util.ByteStringBuilder
import akka.util.ByteIterator

trait Composable[T] {
  def write(t: T, bs: ByteStringBuilder): Unit
  def read(i: ByteIterator): T
}
  