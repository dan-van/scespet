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
class GroupFunc[K,V](source:HasVal[V], keyFunc:V => K, env:types.Env) extends AbstractVectorStream[K,ValueFunc[V], V] with types.MFunc {
  {
    env.addListener(source.trigger, this)
  }

  val getNewColumnTrigger = new ReshapeSignal(env)

  def newCell(i: Int, key: K) = {
    val cell = new ValueFunc[V](source.value, env)  //todo: may not be necessary to do source.value, init later?
    cell.calculate()
    // this cell is now initialised
    getNewColumnTrigger.newColumnAdded(i, true)
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
    var func: ValueFunc[V] = getTrigger(index)
    func.setValue(nextVal)
    return true
  }
}
