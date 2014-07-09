package scespet.core

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 05/04/2013
 * Time: 21:29
 * To change this template use File | Settings | File Templates.
 */
class WindowedReduce[X, Y <: Agg[X]](val dataEvents :HasValue[X], val windowEvents :HasValue[Boolean], newReduce :SliceCellLifecycle[Y], emitType:ReduceType, env :types.Env) extends UpdatingHasVal[Y#OUT] {
    env.addListener(dataEvents.getTrigger, this)
    env.addListener(windowEvents.getTrigger, this)

  var inWindow = windowEvents.value

  var nextReduce : Y = newReduce.newCell()

  var completedReduce : Y = newReduce.newCell()

  def value = if (emitType == ReduceType.CUMULATIVE) nextReduce.value else completedReduce.value

  def calculate():Boolean = {
    var isNowOpen = inWindow
    if (env.hasChanged(windowEvents.getTrigger)) {
      isNowOpen = windowEvents.value
    }
    var fire = false
    if (isNowOpen && !inWindow) {
      // window started
      nextReduce = newReduce.newCell
      inWindow = true
      // cumulative fires on opening edge
      if (emitType == ReduceType.CUMULATIVE) fire = true
    }
    // add data before window close
    var addedValue = false
    if (env.hasChanged(dataEvents.getTrigger)) {
      // note, if the window close coincides with this event, we discard the datapoint
      // i.e. window close takes precedence
      if (isNowOpen) {
        nextReduce.add(dataEvents.value)
        addedValue = true
        if (emitType == ReduceType.CUMULATIVE) fire = true
      }
    }
    if (inWindow && !isNowOpen) {
      // window closed. snap the current reduction and get ready for a new one
      completedReduce = nextReduce
      newReduce.closeCell(completedReduce)
      inWindow = false
      // for ReduceType.LAST, this is the only time we fire, for ReduceType.CUMULATE, only fire a closing edge if we added a value at the same time
      if (emitType == ReduceType.LAST) fire = true
    }
    if (fire) {
      initialised = true
    }
    return fire
  }
}
