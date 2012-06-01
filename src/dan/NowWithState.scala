import collection.mutable.ArrayBuffer

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 10/05/2012
 * Time: 22:52
 * To change this template use File | Settings | File Templates.
 */


class Trade(var price:Double, var qty:Double)
var t:Trade = new Trade(1.2, 100)

trait HasVal[X] {
  def value:X
}

class Stream[X](init:X) extends Func[Null,X] {
  val source = new HasVal[Null] {def value = null}
  def source_= (x:HasVal[Null]):Unit = throw new UnsupportedOperationException("can't set the source of a stream")
  var value = init
  def calculate = true

}

trait Func[X,Y] extends HasVal[Y]{
  var source:HasVal[X]
  def value:Y
  def calculate():Boolean
}

class Expr[X](val source:Expr[_], val target:Func[_, X]) {
  def map[Y](f:X => Y):Expr[Y] = map(new AnonFunc(f))

  def map[Y](f:Func[X,Y]):Expr[Y] = {
    f.source = target
    return new Expr[Y](this, f)
  }
}

abstract class AbsFunc[X,Y] extends Func[X,Y]{
  var source:HasVal[X] = _
  var value:Y = _
}

class AnonFunc[X,Y](val f:X => Y) extends AbsFunc[X,Y]{
  def calculate() = {
    println("Computing "+f+" of "+source.value)
    value=f(source.value);
    true
  }
}

class Sum extends AbsFunc[Double, Double]{
  def calculate() = {value = value + source.value; true}
}

def emitBind(expr:Expr[_]) {
  if (expr.source == null) return
  println(expr.source.target+" -> "+expr.target)
  emitBind(expr.source)
}


class SimpleEvaluator[X](expr:Expr[X]) {
  var funcs:IndexedSeq[Func[_,_]] = new ArrayBuffer()
  val lastFunc = expr.target
  def addToFuncs(expr:Expr[_]) {
    if (expr.source != null) {
      addToFuncs(expr.source)
    }
    funcs = funcs.+:(expr.target)
  }

  addToFuncs(expr)

  def next():X = {
    funcs.foreach(_.calculate())
    lastFunc.value
  }
}

def simpleNext[X](expr:Expr[X]):X = {
  if (expr.source == null) {
    expr.target.calculate()
    return expr.target.value
  } else {
    simpleNext(expr.source)
    expr.target.calculate()
    return expr.target.value
  }
}


var tradeSrc = new Stream( new Trade(1.12, 100) )
var e = new Expr[Trade](null, tradeSrc).map(x=>x.price * x.qty)
emitBind(e)
var simple = new SimpleEvaluator(e)
println( "next val = " + simple.next() )
println
tradeSrc.value = new Trade(2.24, 200)
println( "next val = " + simple.next() )
// this is typesafe and compiles.
// next experiment, thread reference fields