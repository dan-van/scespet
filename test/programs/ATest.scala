package programs

import org.scalatest.junit.{ShouldMatchersForJUnit, JUnitRunner}
import org.junit.runner.RunWith
import org.scalatest.FunSuite

object ATest extends App {
  def computeMagnitude(data:Iterable[Double]):Double = data.map(_.abs).sum
  var a :Iterable[Double] = List(1.0, -1.0)
  var b :Iterable[Double] = Set(1.0, -1.0)
  println( computeMagnitude(a) )
  println( computeMagnitude(b) )
}
