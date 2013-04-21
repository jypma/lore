package info.gamlor.io

import akka.util.ByteString
import akka.actor.IO._

/**
 * @author roman.stoffel@gamlor.info
 * @since 16.03.12
 */

object AdditonalIO {

  /**
   * Same as [[akka.actor.IO#takeUntil]]. However multiple delimiter can be specified.
   * The delimiters are compared in order
   */
  def takeUntil(delimiters: Seq[ByteString], inclusive: Boolean = false): Iteratee[ByteString] = {
    def step(taken: ByteString)(input: Input): (Iteratee[ByteString], Input) = input match {
      case Chunk(more) ⇒
        val bytes = taken ++ more

        for(delimiter <- delimiters){
          val startIdx = bytes.indexOfSlice(delimiter, math.max(taken.length - delimiter.length, 0))
          if (startIdx >= 0) {
            val endIdx = startIdx + delimiter.length
            return (Done(bytes take (if (inclusive) endIdx else startIdx)), Chunk(bytes drop endIdx))
          }
        }
        (Next(step(bytes)), Chunk.empty)
      case EOF  ⇒ (Done(taken), EOF)
      case error @ Error(cause) ⇒ (Failure(cause), error)
    }

    Next(step(ByteString.empty))
  }

}