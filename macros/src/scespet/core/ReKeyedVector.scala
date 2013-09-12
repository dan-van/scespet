package scespet.core

import scespet.core.VectorStream.ReshapeSignal

/**
 * Derive a new multiStream with a new key defintion.
 */
class ReKeyedVector[K,V, K2](source:VectorStream[K,V], keyFunc:K => K2, env:types.Env) extends AbstractVectorStream[K2,ValueFunc[V], V] with types.MFunc {

  env.addListener(source.getNewColumnTrigger, this)
  var lastSourceSize:Int = 0
  // initialise
  calculate()

  val getNewColumnTrigger = new ReshapeSignal(env)

  def newCell(i: Int, key: K2) = {
    ???
  }

  def calculate():Boolean = {
    if (lastSourceSize == source.getSize) return false

    for (i <- lastSourceSize to source.getSize - 1) {
      val k = source.getKey(i)
      val k2 = keyFunc(k)
      var index = getIndex(k2)
      if (index == -1) {
        index = getSize
        add(k2)
      }
      getAt(index)
    }
    lastSourceSize = source.getSize
    true
  }
}
