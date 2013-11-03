package scespet.core

import scespet.core.VectorStream.ReshapeSignal

/**
 * Derive a new multiStream with a new key defintion.
 *
 * todo: ho hum, given that we're potentially multiplexing events, we should either:
 *    introduce a buffer to drain multiplexed events? Yuck, breaks causality in general (yeh, ok, we could special case the 'only one input fired' scenario)
 *    or make a ReKeyedVector implement VectTerm[K2, List[V]]. But then mapping and transforming a List[V] whenever a single V updates is sucky (both from API and performance)
 *    I think
 */
class NestedVector[K2 ,K, V](source:VectorStream[K,V], keyFunc:K => K2, env:types.Env) extends AbstractVectorStream[K2, VectorStream[K,V]](env) with types.MFunc {
  val inputAsTerm = new VectTerm[K,V](env)(source)
  val getNewColumnTrigger = new ReshapeSignal(env)

  env.addListener(source.getNewColumnTrigger, this)
  var lastSourceSize:Int = 0

  // initialise
  calculate()

  class MyCell(val value:VectorStream[K,V]) extends HasVal[VectorStream[K,V]] {
    val trigger = value.getNewColumnTrigger
  }

  def newCell(i: Int, key: K2) = {
    val k2SubVector = inputAsTerm.subset(keyFunc(_) == key)
    new MyCell(k2SubVector.input)
  }

  // called whenever the input vector gets a new key
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
    }
    lastSourceSize = source.getSize
    true
  }
}
