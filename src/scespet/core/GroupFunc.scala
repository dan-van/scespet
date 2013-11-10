package scespet.core

import scespet.core.VectorStream.ReshapeSignal

/**
 * This takes a Stream and demultiplexes it into a VectorStream using a value -> key function
 *
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:14
 * To change this template use File | Settings | File Templates.
 */
// this one uses pur function calls and tracks updated indicies.
// we could try a verison that uses wakeup nodes.
class GroupFunc[K,V](source:HasVal[V], keyFunc:V => K, env:types.Env) extends AbstractVectorStream[K, V](env) with types.MFunc {
  {
    env.addListener(source.trigger, this)
  }

  val getNewColumnTrigger = new ReshapeSignal(env)

  def newCell(i: Int, key: K) = {
    val cell = new ValueFunc[V](env)
    // initialise it
    cell.calculate()
    cell
  }

  def calculate():Boolean = {
    val nextVal: V = source.value
    val key: K = keyFunc.apply(nextVal)
    var index: Int = getIndex(key)
    if (index == -1) {
      index = getSize()
      add(key)
    }
    var func = getValueHolder(index).asInstanceOf[ValueFunc[V]]
    func.setValue(nextVal)
    return true
  }
}
