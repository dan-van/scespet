package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.util.Logged
import scespet.core.types.MFunc


/**
 * todo: remove code duplication with SlicedReduce
 *  event wiring
 *
 * joinInputs--+
 *             |
 *         joinRendezvous -+-> nextReduce -> SliceAfterBucket
 *                         \                  /
 *                          +----------------+
 * sliceEvent ------------------------------/
 *
 *
 */
 
class SliceAfterBucket[S, Y, OUT](cellOut:AggOut[Y,OUT], val sliceSpec :S, cellLifecycle :SliceCellLifecycle[Y], emitType:ReduceType, env :types.Env, ev: SliceTriggerSpec[S]) extends SlicedBucket[Y, OUT] {
  var awaitingNextEventAfterReset = false   // start as false so that initialisation is looking at nextReduce.value. May need more thought

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
        // make sure we wake up to consume this
        // I've chosen 'fireAfterListeners' as I'm worried that more input sources may fire, and since we've not expressed causality
        // relationships, we could fire the nextReduce before that is all complete.
        // it also seems right, we establish all listeners on the new binding before it is fired
        // maybe if this needs to change, we should condition on whether this is a new bucket (which would be fireAfterListeners)
        // or if this is an existing bucket (which should be wakeupThisCycle)
        if (cellIsFunction) {
          env.fireAfterChangingListeners(nextReduce.asInstanceOf[MFunc])
        } else {
          env.fireAfterChangingListeners(SliceAfterBucket.this)
        }
      }
    }
  }
  env.addListener(joinValueRendezvous, this)


  private def resetCurrentReduce() {
    if (nextReduce != null) {
      cellLifecycle.closeCell(nextReduce)
      completedReduceValue = cellOut.out(nextReduce)  // Intellij thinks this a a compile error - it isn't
      cellLifecycle.reset(nextReduce)
    }
    awaitingNextEventAfterReset = true
  }

  private val nextReduce : Y = cellLifecycle.newCell()
  private val cellIsFunction :Boolean = if (nextReduce.isInstanceOf[MFunc]) {
    // join values trigger the bucket
    env.addListener(joinValueRendezvous, nextReduce.asInstanceOf[MFunc])
    // listen to it so that we propagate value updates to the bucket
    env.addListener(nextReduce, this)

//    // TODO: if nextReduce was a hasVal, then we'd have strong modelling of initialisation state
//    env.fireAfterChangingListeners(nextReduce.asInstanceOf[MFunc])
    true
  } else {
    false
  }

  private var completedReduceValue : OUT = _

  // if awaitingNextEventAfterReset then the nextReduce has been reset, and we should be exposing the last snap (even if we're in CUMULATIVE mode)
  def value :OUT = if (emitType == ReduceType.LAST || awaitingNextEventAfterReset) completedReduceValue else cellOut.out(nextReduce)
//  initialised = value != null
  initialised = false // todo: hmm, for CUMULATIVE reduce, do we really think it is worth pushing our state through subsequent map operations?
                      // todo: i.e. by setting initialised == true, we actually fire an event on construction of an empty bucket

  def addInputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
    joinValueRendezvous.addInputBinding(in, adder)
  }

  // nextReduce fires when a value event has occured, so we pass it to the sliceTrigger builder
  private val eventCountInput = if (cellIsFunction) Set(nextReduce.asInstanceOf[EventGraphObject]) else joinValueRendezvous.inputBindings.keySet
  var sliceEvents :types.EventGraphObject = ev.buildTrigger(sliceSpec, eventCountInput, env)
  // wire up slice listening:
  if (sliceEvents != null) {
    env.addListener(sliceEvents, this)
  }

  // by 'slicing' on termination, it means we expose partial buckets. Is this a good thing?
  // hmm, maybe I should just make the sliceTrigger be responsible for bringing in termination?
  private val termination = env.getTerminationEvent
  if (emitType == ReduceType.LAST) {
    env.addListener(termination, this)
  }

  private class InputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
    def addValueToBucket(bucket:Y) {
      adder(bucket)(in.value)
    }
  }

  def calculate():Boolean = {
    if (awaitingNextEventAfterReset) {
      if (env.hasChanged(nextReduce)) {
        // got one.
        awaitingNextEventAfterReset = false
      }
    }

    val bucketFire = if (emitType == ReduceType.CUMULATIVE) {
      if (cellIsFunction) env.hasChanged(nextReduce) else env.hasChanged(joinValueRendezvous)
    } else {
      false
    }
    val sliceFire = if (sliceTriggered()) {
      resetCurrentReduce()
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

