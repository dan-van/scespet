package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.util.Logged


/**
 * todo: remove code duplication with SlicedReduce
 *  event wiring
 *
 * joinInputs--+
 *             |
 *         joinRendezvous -+-> nextReduce -> SliceAfterBucket
 *             |           \                  /
 *             |            +----------------+
 * sliceEvent -+----------------------------/
 *
 *
 */
 
class SliceAfterBucket[S, Y <: Bucket](val sliceSpec :S, cellLifecycle :SliceCellLifecycle[Y], emitType:ReduceType, env :types.Env, ev: SliceTriggerSpec[S]) extends SlicedBucket[Y] {
  // most of the work is actually handled in this 'rendezvous' class
  private val joinValueRendezvous = new types.MFunc {
    var inputBindings = Map[EventGraphObject, InputBinding[_]]()

    def calculate(): Boolean = {
      // hmm, I should probably provide a dumb implementation of this API call in case we have many inputs...
      import collection.JavaConversions.iterableAsScalaIterable
      var addedValueToBucket = false
      for (t <- env.getTriggers(this)) {
        val option = inputBindings.get(t)
        if (option.isDefined) {
          option.get.addValueToBucket(nextReduce)
          addedValueToBucket = true
        }
      }
      addedValueToBucket
    }

    def addInputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
      val inputBinding = new InputBinding[X](in, adder)
      val trigger = in.getTrigger
      inputBindings += trigger -> inputBinding
      env.addListener(trigger, this)
      if (in.initialised) {
        inputBinding.addValueToBucket(nextReduce)
        // make sure we fire the target bucket for this
        // I've chosen 'fireAfterListeners' as I'm worried that more input sources may fire, and since we've not expressed causality
        // relationships, we could fire the nextReduce before that is all complete.
        // it also seems right, we establish all listeners on the new binding before it is fired
        // maybe if this needs to change, we should condition on whether this is a new bucket (which would be fireAfterListeners)
        // or if this is an existing bucket (which should be wakeupThisCycle)
        env.fireAfterChangingListeners(nextReduce)
      }
    }
  }

  // not 100% sure about this - if we are only emitting completed buckets, we close and emit a bucket when the system finishes
  private val termination = env.getTerminationEvent
  if (emitType == ReduceType.LAST) {
    env.addListener(termination, joinValueRendezvous)
  }

  env.addListener(joinValueRendezvous, this)


  private def closeCurrentBucket() {
    if (nextReduce != null) {
      cellLifecycle.closeCell(nextReduce)
      completedReduceValue = nextReduce.value
    }
  }

  // NOTE: closeCurrentBucket should always be called before this!
  private def readyNextReduce() {
    cellLifecycle.reset(nextReduce)
  }

  private val nextReduce : Y = cellLifecycle.newCell()
  // join values trigger the bucket
  env.addListener(joinValueRendezvous, nextReduce)
  // listen to it so that we propagate value updates to the bucket
  env.addListener(nextReduce, this)


  private var completedReduceValue : Y#OUT = _

  def value = if (emitType == ReduceType.CUMULATIVE) nextReduce.value else completedReduceValue
//  initialised = value != null
  initialised = false // todo: hmm, for CUMULATIVE reduce, do we really think it is worth pushing our state through subsequent map operations?
                      // todo: i.e. by setting initialised == true, we actually fire an event on construction of an empty bucket

  def addInputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
    joinValueRendezvous.addInputBinding(in, adder)
  }

  var sliceEvents :types.EventGraphObject = _
  def bindingsComplete() {
    sliceEvents = ev.buildTrigger(sliceSpec, Set(nextReduce), env)
    // wire up slice listening:
    if (sliceEvents != null) {
      env.addListener(sliceEvents, this)
    }
  }

  private class InputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
    def addValueToBucket(bucket:Y) {
      adder(bucket)(in.value)
    }
  }

  def calculate():Boolean = {
    val bucketFire = if (emitType == ReduceType.CUMULATIVE) {
      env.hasChanged(nextReduce)
    } else {
      false
    }
    val sliceFire = if (sliceTriggered()) {
      closeCurrentBucket()
      // NOTE: we could lazily ready next reduce if we had to
      readyNextReduce()
      true
    } else {
      false
    }

    val fire = bucketFire || sliceFire
    if (fire)
      initialised = true  // belt and braces initialiser
    return fire
  }

  def sliceTriggered() :Boolean = {
    if (sliceEvents != null && env.hasChanged(sliceEvents)) return true
    if (emitType == ReduceType.LAST && env.hasChanged(termination)) return true
    false
  }
}

