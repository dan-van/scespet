package scespet.core

import gsa.esg.mekon.core.EventGraphObject.Lifecycle

/**
 * Hmm, this was an initial version of reducing. It is simpler (and probably more efficient) than the SlicedBucket implementations, as it does not try to do a rendezvous of incoming event streams
 */
class SlicedReduce[S, X, Y <: Cell](val dataEvents :HasValue[X], val cellValueAdd:CellAdder[Y, X], val sliceSpec :S, val sliceBefore:Boolean, cellLifecycle :SliceCellLifecycle[Y], emitType:ReduceType, env :types.Env, sliceBuilder: SliceTriggerSpec[S]) extends UpdatingHasVal[Y#OUT] {
  var newSliceNextEvent = false
  val sliceEvents = sliceBuilder.buildTrigger(sliceSpec, Set(dataEvents.getTrigger), env)
  
  env.addListener(dataEvents.getTrigger, this)
  if (sliceEvents != null) env.addListener(sliceEvents, this)

  val termination = env.getTerminationEvent
  if (emitType == ReduceType.LAST) {
    env.addListener(termination, this)
  }

  private var nextReduce : Y = _
  def readyNextReduceCell() {
    nextReduce = cellLifecycle.newCell()
  }
  readyNextReduceCell()

  var completedReduceValue : Y#OUT = _ // or should this be instantiated?

  def value = if (emitType == ReduceType.CUMULATIVE) nextReduce.value else completedReduceValue
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
      cellLifecycle.closeCell(nextReduce)
      completedReduceValue = nextReduce.value
      readyNextReduceCell()
      newSliceNextEvent = false
      // just sliced, don't slice again!
      sliceTrigger = false
    }
    if (env.hasChanged(dataEvents.getTrigger)) {
      val newValue = dataEvents.value
      cellValueAdd.addTo(nextReduce, newValue)
    }
    if (!sliceBefore && sliceTrigger) {
      newSliceNextEvent = true
      if (emitType == ReduceType.LAST) {
        cellLifecycle.closeCell(nextReduce)
        completedReduceValue = nextReduce.value
        fire = true
      }
    }
    if (fire) initialised = true  // belt and braces
    return fire
  }
}
