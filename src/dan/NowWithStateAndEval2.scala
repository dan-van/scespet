import collection.mutable.ArrayBuffer

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 10/05/2012
 * Time: 22:52
 * To change this template use File | Settings | File Templates.
 */
/**
 * Something that provides a value (i.e. a source)
 * @tparam X
 */
trait HasVal[X] {
  def value:X
}

/**
 * something that has a source, and is a source (i.e. a pipe)
 * on "calculate", it may take from its source, and update its yielded value
 * if calculate returns false, then state was not modified
 *
 * i.e. this is an operation that can both map and filter
 * @tparam X
 * @tparam Y
 */
trait Func[X,Y] extends HasVal[Y]{
  var source:HasVal[X]
  def value:Y
  def calculate():Boolean
}

trait FuncCollector{
  def bind(src:Func[_,_], sink:Func[_,_])
}

/**
 * this represents a binding of source -> sink
 * it provides operations to build subsequent bindings (e.g. "map")
 * it notifies a FuncCollector about all constructed bindings
 *
 * @param source
 * @param target
 * @param collector
 * @tparam X
 */
class Expr[X](val source:Func[_,_], val target:Func[_, X])(implicit collector:FuncCollector) {
  collector.bind(source, target)

  def map[Y](f:X => Y):Expr[Y] = map(new AnonFunc(f))

  def map[Y](f:Func[X,Y]):Expr[Y] = {
    f.source = target
    return new Expr[Y](this.target, f)
  }
}



// a Func with vars for storage
abstract class AbsFunc[X,Y] extends Func[X,Y]{
  var source:HasVal[X] = _
  var value:Y = _
}

// a Func built from a Function1
class AnonFunc[X,Y](val f:X => Y) extends AbsFunc[X,Y]{
  def calculate() = {
    print("Computing "+f+" of "+source.value)
    value=f(source.value)
    println(" = "+value)
    true
  }
}

// I think this is a bit duff, basically it is a root "pipe"
class Stream[X](val sType:Class[X]) extends Func[Null,X] {
  val source = new HasVal[Null] {def value = null}
  def source_= (x:HasVal[Null]):Unit = throw new UnsupportedOperationException("can't set the source of a stream")
  var value:X = _
  def calculate = true
}

// I think this is a bit duff, basically it is a root "pipe"
class IteratorAsFunc[X](val iterable:Iterable[X]) extends Func[Null,X] {
  val iterator = iterable.iterator
  val source = new HasVal[Null] {def value = null}
  def source_= (x:HasVal[Null]):Unit = throw new UnsupportedOperationException("can't set the source of a stream")
  var value:X = _
  def calculate = {if (iterator.hasNext) {value = iterator.next(); true} else false}
}


class SimpleEvaluator() extends FuncCollector {
  var funcs:IndexedSeq[Func[_,_]] = new ArrayBuffer()

  def add[X](stream:Stream[X]):Expr[X] = {
    new Expr(null, stream)(this)
  }

  def add[X](iterator:Iterator[X]):Expr[X] = {
    var asFunc = new IteratorAsFunc(iterator)
    new Expr(null, asFunc)(this)
  }

  def bind(src: Func[_, _], sink: Func[_, _]) {
    println("bound "+src+" -> "+sink)
    funcs = funcs :+ sink
  }

  def advance() = {
    funcs.foreach(_.calculate())
  }
}


// ------ test it --------

class Sum extends AbsFunc[Double, Double]{
  def calculate() = {value = value + source.value; true}
}

var simple = new SimpleEvaluator()

class Trade(var price:Double, var qty:Double)
var tradeIter = new ArrayBuffer[Trade]()
tradeIter += new Trade(1.12, 100)
tradeIter += new Trade(2.12, 200)
tradeIter += new Trade(3.12, 100)
tradeIter += new Trade(4.12, 200)
tradeIter += new Trade(5.12, 100)

simple.add(tradeIter).map().map(println())
simple.run()
