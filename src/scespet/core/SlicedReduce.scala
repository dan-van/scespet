package scespet.core

import gsa.esg.mekon.core.EventGraphObject.Lifecycle

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 05/04/2013
 * Time: 21:29
 * To change this template use File | Settings | File Templates.
 */
class SlicedReduce[X, Y <: Reduce[X]](val dataEvents :HasValue[X], val sliceEvents :types.EventGraphObject, val sliceBefore:Boolean, newReduce :()=>Y, emitType:ReduceType, env :types.Env) extends UpdatingHasVal[Y] with Lifecycle {
  var newSliceNextEvent = false

  env.addListener(dataEvents.getTrigger, this)
  if (sliceEvents != null) env.addListener(sliceEvents, this)

  var nextReduce : Y = newReduce()

  var completedReduce : Y = _

  def value = if (emitType == ReduceType.CUMULATIVE) nextReduce else completedReduce

  def init() {}

  def destroy() {
    // todo: maybe it would be better to have a terminationEvent that this sort of thing could listen to?
    // todo: not sure if we should *always* expose next bucket on terminate? maybe only on reduce-all?
    completedReduce = nextReduce
  }

  def calculate():Boolean = {
    var fire = emitType == ReduceType.CUMULATIVE // every cumulative event is exposed

    val sliceTrigger = (sliceEvents != null && env.hasChanged(sliceEvents))
    if (newSliceNextEvent || (sliceBefore && sliceTrigger)) {
      completedReduce = nextReduce
      nextReduce = newReduce()
      newSliceNextEvent = false
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
