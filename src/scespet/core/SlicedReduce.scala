package scespet.core

import gsa.esg.mekon.core.EventGraphObject.Lifecycle

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 05/04/2013
 * Time: 21:29
 * To change this template use File | Settings | File Templates.
 */
class SlicedReduce[X, Y <: Reduce[X]](val dataEvents :HasValue[X], val sliceEvents :types.EventGraphObject, val sliceBefore:Boolean, newReduce :()=>Y, env :types.Env) extends UpdatingHasVal[Y] with Lifecycle {
  env.addListener(dataEvents.getTrigger, this)
  if (sliceEvents != null) env.addListener(sliceEvents, this)

  var nextReduce : Y = newReduce()

  var completedReduce : Y = _

  def value = completedReduce

  def init() {}

  def destroy() {
    // todo: maybe it would be better to have a terminationEvent that this sort of thing could listen to?
    completedReduce = nextReduce
  }

  def calculate():Boolean = {
    // add data before window close
    var fire = false

    if (sliceBefore) {
      if (sliceEvents != null && env.hasChanged(sliceEvents)) {
        completedReduce = nextReduce
        nextReduce = newReduce()
        fire = true
      }
    }
    if (env.hasChanged(dataEvents.getTrigger)) {
      nextReduce.add(dataEvents.value)
    }
    if (!sliceBefore) {
      if (sliceEvents != null && env.hasChanged(sliceEvents)) {
        completedReduce = nextReduce
        nextReduce = newReduce()
        fire = true
      }
    }
    return fire
  }
}
