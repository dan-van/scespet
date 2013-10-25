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
class ReKeyedVector[K,V, K2](source:VectorStream[K,V], keyFunc:K => K2, env:types.Env) extends AbstractVectorStream[K2, V] with types.MFunc {
  val getNewColumnTrigger = new ReshapeSignal(env)

  env.addListener(source.getNewColumnTrigger, this)
  var lastSourceSize:Int = 0

  // initialise
  calculate()

  def newCell(i: Int, key: K2) = {
    ???
    // build an event multiplexer? Or what about formalising this concept in core graph, letting the graph
    // have a special class of Function that can provide multiple events? Then we'd drain them atomically in the graph walk
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
      val mergeCell = getValueHolder(index)
//      mergeCell.addInputStream(i)
    }
    lastSourceSize = source.getSize
    true
  }
}
