package net.ypmania.test

import org.scalatest.matchers.{ Matcher, MatchResult }
import scala.reflect.runtime.universe._

trait ExtraMatchers {
  
  private val m = runtimeMirror(getClass.getClassLoader)

  def beOfType[T:TypeTag] = Matcher { obj: Any =>
    MatchResult(
      obj.getClass == m.runtimeClass(typeOf[T]),
      obj.toString + " was not an instance of " + typeOf[T],
      obj.toString + " was an instance of " + typeOf[T])
  }
}