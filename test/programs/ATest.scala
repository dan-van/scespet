package programs

import org.scalatest.junit.{ShouldMatchersForJUnit, JUnitRunner}
import org.junit.runner.RunWith
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class ATest extends FunSuite with ShouldMatchersForJUnit {
  test("testing ") {
    1 should be(1)
  }
  test("testing 2") {
    1 should be(1)
  }
}
