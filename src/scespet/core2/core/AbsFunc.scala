package scespet.core2.core

// a Func with vars for storage
abstract class AbsFunc[X, Y] extends Func[X, Y] {
  var source: HasVal[X] = _
  var value: Y = _
}
