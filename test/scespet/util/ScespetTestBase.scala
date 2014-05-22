package scespet.util

import org.scalatest.junit.{ShouldMatchersForJUnit, AssertionsForJUnit}
import scespet.core.Term
import org.scalatest.FunSuite

/**
 * Created by danvan on 21/05/2014.
 */
// it's a class as Scalatest author says they're much faster to compile than traits and I don't think I'll need to multiply mix this in
class ScespetTestBase extends FunSuite with AssertionsForJUnit with ShouldMatchersForJUnit {
  class StreamTest[X](name:String, expected:Iterable[X], stream:Term[X]) {
    var eventI = 0
    val expectIter = expected.iterator
    stream.map(next => {
      assert(expectIter.hasNext, s"Stream $name, Event $eventI with value $next was additional to expected")
      val expect = expectIter.next()
      expectResult(expect, s"Stream $name, Event $eventI was not expected")(next)
      println(s"Observed event: $name-$eventI \t $next as expected")
      eventI += 1
    })
  }
}
