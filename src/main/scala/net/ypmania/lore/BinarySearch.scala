package net.ypmania.lore

import scala.annotation.tailrec
import scala.reflect.ClassTag

object BinarySearch {
  implicit class SearchableSeq[T: ClassTag: Ordering](seq: IndexedSeq[T]) {
    def binarySearch(needle: T): Int = {
      @tailrec
      def binarySearch(low: Int, high: Int): Int = {
        if (low <= high) {
          val middle = low + (high - low) / 2

          if (seq(middle) == needle)
            middle
          else if (Ordering[T].lt (seq(middle), needle))
            binarySearch(middle + 1, high)
          else
            binarySearch(low, middle - 1)
        } else
          -(low + 1)
      }

      binarySearch(0, seq.length - 1)
    }
  }
}
