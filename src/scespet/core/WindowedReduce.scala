package scespet.core

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 05/04/2013
 * Time: 21:29
 * To change this template use File | Settings | File Templates.
 */
class WindowedReduce[X, Y <: Reduce[X]](val dataEvents :HasValue[X], val windowEvents :HasValue[Boolean], newReduce :()=>Y, emitType:ReduceType, env :types.Env) extends UpdatingHasVal[Y] {
    env.addListener(dataEvents.getTrigger, this)
    env.addListener(windowEvents.getTrigger, this)

  var inWindow = windowEvents.value

  var nextReduce : Y = newReduce()

  var completedReduce : Y = _

  def value = if (emitType == ReduceType.CUMULATIVE) nextReduce else completedReduce

  def calculate():Boolean = {
    // add data before window close
    var fire = emitType == ReduceType.CUMULATIVE

    var isNowOpen = inWindow
    if (env.hasChanged(windowEvents.getTrigger)) {
      isNowOpen = windowEvents.value
    }
    if (isNowOpen && !inWindow) {
      // window started
      nextReduce = newReduce()
      inWindow = true
    }
    if (env.hasChanged(dataEvents.getTrigger)) {
      // note, if the window close coincides with this event, we discard the datapoint
      // i.e. window close takes precedence
      if (isNowOpen) {
        nextReduce.add(dataEvents.value)
      }
    }
    if (inWindow && !isNowOpen) {
      // window closed. snap the current reduction and get ready for a new one
      completedReduce = nextReduce
      inWindow = false
      fire = true
    }
    return fire
  }
}
