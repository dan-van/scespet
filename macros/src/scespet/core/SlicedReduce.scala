package scespet.core

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 05/04/2013
 * Time: 21:29
 * To change this template use File | Settings | File Templates.
 */
class SlicedReduce[X, Y <: Reduce[X]](val dataEvents :HasValue[X], val sliceEvents :types.EventGraphObject, val sliceBefore:Boolean, newReduce :()=>Y, env :types.Env) extends UpdatingHasVal[Y] {
    env.addListener(dataEvents.getTrigger, this)
    env.addListener(sliceEvents, this)

    var nextReduce : Y = newReduce()

    var completedReduce : Y = _

    def value = completedReduce

  def calculate():Boolean = {
    // add data before window close
    var fire = false

    if (sliceBefore) {
      if (env.hasChanged(sliceEvents)) {
        completedReduce = nextReduce
        nextReduce = newReduce()
        fire = true
      }
    }
    if (env.hasChanged(dataEvents.getTrigger)) {
      nextReduce.add(dataEvents.value)
    }
    if (!sliceBefore) {
      if (env.hasChanged(sliceEvents)) {
        completedReduce = nextReduce
        nextReduce = newReduce()
        fire = true
      }
    }
    return fire
  }
}
