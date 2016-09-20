package scespet.util

import scespet.core.{SelfAgg, Reducer, Agg}

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 28/03/2013
 * Time: 10:46
 * To change this template use File | Settings | File Templates.
 */
class Sum[X:Numeric] extends Reducer[X, Double]{
  var sum = 0.0
  def value = sum
  def add(n:X):Unit = {sum = sum + implicitly[Numeric[X]].toDouble(n)}

  override def toString = s"Sum=$sum"
}

class Avg[X:Numeric] extends Reducer[X, Double]{
  var sum = 0.0
  var n = 0
  def value = sum / n
  def add(x:X):Unit = {
    sum = sum + implicitly[Numeric[X]].toDouble(x)
    n += 1
  }

  override def toString = s"Sum=$sum"
}


class EWMA(val lambda:Double = 0.98) extends SelfAgg[Int]{
  var s:Double = 0;
  def add(i:Int):Unit = { s = Math.pow(i, 1-lambda) + Math.pow(s, lambda) }

  override def toString = s"EWMA=$s"
}

class UberEWMA extends EWMA {
  var n:Int = 0
  override def add(i: Int): Unit = {
    super.add(i)
    n += 1
  }
  override def toString = s"Sum=$s, N=$n"
}

class Collect extends SelfAgg[AnyRef] {
  val data = collection.mutable.Buffer[AnyRef]()
  def add(x: AnyRef) {data += x}
}

/**
 * todo: how can we genericise Counter to allow type inference to fill in the type param here?
 * @tparam T
 */
class Counter extends Reducer[Any, Int] {
  var c=0
  override def add(x: Any) {
    c += 1
  }
  def value = c
  override def toString = String.valueOf(c)
}

//object Foo {
//  val c = new Counter2()
//  c.add(1)
//}