package scespet.core

import gsa.esg.mekon.core.EventGraphObject.Lifecycle

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 05/04/2013
 * Time: 21:29
 * To change this template use File | Settings | File Templates.
 */
class SlicedReduce[X, Y <: Agg[X]](val dataEvents :HasValue[X], val sliceEvents :types.EventGraphObject, val sliceBefore:Boolean, newReduce :()=>Y, emitType:ReduceType, env :types.Env) extends UpdatingHasVal[Y#OUT] {
  var newSliceNextEvent = false

  env.addListener(dataEvents.getTrigger, this)
  if (sliceEvents != null) env.addListener(sliceEvents, this)

  val termination = env.getTerminationEvent
  if (emitType == ReduceType.LAST) {
    env.addListener(termination, this)
  }

  var nextReduce : Y = newReduce()
  var completedReduce : Y = newReduce()

  def value = if (emitType == ReduceType.CUMULATIVE) nextReduce.value else completedReduce.value
//  initialised = value != null
  initialised = false // todo: hmm, for CUMULATIVE reduce, do we really think it is worth pushing our state through subsequent map operations?
                      // todo: i.e. by setting initialised == true, we actually fire an event on construction of an empty bucket

  def calculate():Boolean = {
    var fire = emitType == ReduceType.CUMULATIVE // every cumulative event is exposed
    if (emitType == ReduceType.LAST && env.hasChanged(termination)) {
      newSliceNextEvent = true
      fire = true
    }
    // build a new bucket if necessary
    var sliceTrigger = (sliceEvents != null && env.hasChanged(sliceEvents))
    if (sliceBefore && sliceTrigger) {
      newSliceNextEvent = true
      fire = true
    }
    if (newSliceNextEvent) {
      completedReduce = nextReduce
      nextReduce = newReduce()
      newSliceNextEvent = false
      // just sliced, don't slice again!
      sliceTrigger = false
    }
    if (env.hasChanged(dataEvents.getTrigger)) {
      nextReduce.add(dataEvents.value)
    }
    if (!sliceBefore && sliceTrigger) {
      newSliceNextEvent = true
      if (emitType == ReduceType.LAST) {
        completedReduce = nextReduce
        fire = true
      }
    }
    if (fire) initialised = true  // belt and braces
    return fire
  }
}
