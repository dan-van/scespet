package programs

import scespet.core.Reduce

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 28/03/2013
 * Time: 10:46
 * To change this template use File | Settings | File Templates.
 */
class Sum extends Reduce[Int]{
  var s = 0;
  def add(i:Int):Unit = {println(s"Adding $i to sum:$s = ${s+i}");s += i}

  override def toString = s"Sum=$s"
}
class Counter[T <: Any] extends Reduce[T] {
  var c=0
  def add(x: T) { c += 1 }

  override def toString = String.valueOf(c)
}
