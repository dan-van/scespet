package scespet.core

import java.util

import gsa.esg.mekon.core.EventGraphObject
import gsa.esg.mekon.core.EventGraphObject.Lifecycle

/**
 * Hmm, this was an initial version of reducing. It is simpler (and probably more efficient) than the SlicedBucket implementations, as it does not try to do a rendezvous of incoming event streams
 */
class SlicedReduce[S, X, Y, OUT](val dataEvents :HasValue[X], val cellValueAdd:Y => CellAdder[X], cellOut:AggOut[Y,OUT], val sliceSpec :S, val sliceBefore:Boolean, cellLifecycle :SliceCellLifecycle[Y], emitType:ReduceType, env :types.Env, sliceBuilder: SliceTriggerSpec[S], exposeInitialValue:Boolean) extends UpdatingHasVal[OUT] with EventGraphObject.Lifecycle {
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

  var completedReduceValue : OUT = _ // or should this be instantiated?

  def value = if (emitType == ReduceType.CUMULATIVE) cellOut.out(nextReduce) else completedReduceValue


//  override def init(initialisedInputs: util.Collection[EventGraphObject]): Boolean = {
  override def init(): Unit = {
//    if (dataEvents.initialised()) {
//      initialised = true
//      val newValue = dataEvents.value
//      cellValueAdd(nextReduce).add(newValue)
//      true
//    } else {
//      false
//    }
    true
  }

  if (emitType == ReduceType.LAST) {
    initialised = false
  } else {
    // hmm, interesting implications here.
    // a CUMULATIVE reduce will be pushing out state changes for each new datapoint.
    // the question is, is the state valid for downstream map/filter/join before that first datapoint has arrived?
    // i.e. are we happy exposing the emptystate of nextReduce?
    // maybe this answer is up to the implementation of Y?

    // NODEPLOY think about this further, but I'm going with nextReduce is in valid state now.
    // todo: maybe we could tweak this if Y instanceof something with initialisation state?
    initialised = false
  }

  override def destroy(): Unit = {}

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
      completedReduceValue = cellOut.out(nextReduce)
      readyNextReduceCell()
      newSliceNextEvent = false
      // just sliced, don't slice again!
      sliceTrigger = false
    }
    if (env.hasChanged(dataEvents.getTrigger)) {
      val newValue = dataEvents.value
      cellValueAdd(nextReduce).add(newValue)
    }
    if (!sliceBefore && sliceTrigger) {
      newSliceNextEvent = true
      if (emitType == ReduceType.LAST) {
        cellLifecycle.closeCell(nextReduce)
        completedReduceValue = cellOut.out(nextReduce)
        fire = true
      }
    }
    if (fire) initialised = true  // belt and braces
    return fire
  }
}
