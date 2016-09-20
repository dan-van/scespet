package scespet.core

import scespet.core.VectorStream.ReshapeSignal
import gsa.esg.mekon.core.EventGraphObject

/**
 * Derive a new multiStream with a new key defintion. key def yields option, and if None, then the key is dropped
 */
class ReKeyedVector[K,V, K2](source:VectorStream[K,V], keyFunc:K => Option[K2], env:types.Env) extends AbstractVectorStream[K2, V](env) with types.MFunc {
  def isInitialised: Boolean = source.isInitialised

  val getNewColumnTrigger = new ReshapeSignal(env, this)
  env.addListener(source.getNewColumnTrigger, this)
  // this function can add things to the reshape signal
  env.addWakeupReceiver(this, getNewColumnTrigger)
  if (source.isInitialised) {
    env.wakeupThisCycle(this)
  }

  var lastSourceSize:Int = 0
  val sourceIndicies = collection.mutable.ArrayBuffer[Int]()


  def newCell(newIndex: Int, key: K2) = {
    val sourceIndex = sourceIndicies(newIndex)
    val sourceCell = source.getValueHolder(sourceIndex)
    val sourceTrigger: EventGraphObject = sourceCell.getTrigger()

    val hasInputValue = sourceCell.initialised()
    val hasChanged = env.hasChanged(sourceTrigger)
    if (hasChanged && !hasInputValue) {
      println("WARN: didn't expect this")
    }
    // NOTE: yes, I'm returning the actual HasValue from the other vector, Maybe this is dangerous, and maybe I should chain them up?
    sourceCell
  }

  // called whenever the input vector gets a new key
  def calculate():Boolean = {
    if (lastSourceSize == source.getSize) return false

    for (i <- lastSourceSize to source.getSize - 1) {
      val newKey = source.getKey(i)
      val k2 = keyFunc(newKey)
      if (k2.isDefined) {
        val index = getIndex(k2.get)
        if (index == -1) {
          sourceIndicies += i
          add(k2.get)
        } else {
          // there is already a mapping to K2, to support this we'd need to
          // do some sort of multiplexing operation (e.g. buffering, or 'multi-event graph propagation'
          val existingIdx = sourceIndicies(index)
          val oldK = source.getKey(existingIdx)
          throw new UnsupportedOperationException("Multiple keys map to "+k2.get+". First is "+oldK+", new is "+newKey)
        }
      } // else ignore this one
    }
    lastSourceSize = source.getSize
    true
  }
}
