package scespet.util

import scespet.core.Reduce

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 28/03/2013
 * Time: 10:46
 * To change this template use File | Settings | File Templates.
 */
class Sum extends Reduce[Int]{
  var s = 0
  def add(n:Int):Unit = {s = n + s}

  override def toString = s"Sum=$s"
}


class EWMA(val lambda:Double = 0.98) extends Reduce[Int]{
  var s:Double = 0;
  def add(i:Int):Unit = { s = Math.pow(i, 1-lambda) + Math.pow(s, lambda) }

  override def toString = s"Sum=$s"
}

/**
 * todo: how can we genericise Counter to allow type inference to fill in the type param here?
 * @tparam T
 */
class Counter[T] extends Reduce[T] {
  var c=0
  def add(x: T) { c += 1 }

  override def toString = String.valueOf(c)
}
class Counter2 extends Reduce[Any] {
  var c=0
  def add(x: Any) { c += 1 }

  override def toString = String.valueOf(c)
}

//object Foo {
//  val c = new Counter2()
//  c.add(1)
//}