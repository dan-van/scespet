package scespet.core

import gsa.esg.mekon.core.EventGraphObject.Lifecycle

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 05/04/2013
 * Time: 21:29
 * To change this template use File | Settings | File Templates.
 */
class SlicedReduce[X, Y <: Reduce[X]](val dataEvents :HasValue[X], val sliceEvents :types.EventGraphObject, val sliceBefore:Boolean, newReduce :()=>Y, emitType:ReduceType, env :types.Env) extends UpdatingHasVal[Y] {
  var newSliceNextEvent = false

  env.addListener(dataEvents.getTrigger, this)
  if (sliceEvents != null) env.addListener(sliceEvents, this)

  val termination = env.getTerminationEvent
  if (emitType == ReduceType.LAST) {
    env.addListener(termination, this)
  }

  var nextReduce : Y = newReduce()
  var completedReduce : Y = _

  def value = if (emitType == ReduceType.CUMULATIVE) nextReduce else completedReduce

  def calculate():Boolean = {
    var fire = emitType == ReduceType.CUMULATIVE // every cumulative event is exposed
    if (emitType == ReduceType.LAST && env.hasChanged(termination)) {
      newSliceNextEvent = true
    }
    var sliceTrigger = (sliceEvents != null && env.hasChanged(sliceEvents))
    if (newSliceNextEvent || (sliceBefore && sliceTrigger)) {
      completedReduce = nextReduce
      nextReduce = newReduce()
      newSliceNextEvent = false
      // just sliced, don't slice again!
      sliceTrigger = false
      fire = true
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
    return fire
  }
}
