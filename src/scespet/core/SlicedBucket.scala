package scespet.core


import scespet.core.MultiVectorJoin.BucketCell
import gsa.esg.mekon.core.EventGraphObject


/**
 * todo: remove code duplication with SlicedReduce
 *  event wiring for slice before:
 *
 *         OtherEvents _
 *                      |
 *    joinRendezvous -> nextReduce -> this
 *               |   |                 |
 * sliceEvent ---     -----------------
 *
 *
 *  event wiring for slice after:
 *
 *         OtherEvents _
 *                      |
 *    joinRendezvous -> nextReduce -> this
 *                                    |
 *                      sliceEvent ---
 *
 */
 
class SlicedBucket[Y <: Bucket](val sliceEvents :types.EventGraphObject, val sliceBefore:Boolean, newReduce :()=>Y, emitType:ReduceType, env :types.Env) extends UpdatingHasVal[Y] with BucketCell[Y] {
  private val joinValueRendezvous = new types.MFunc {
    var inputBindings = Map[EventGraphObject, InputBinding[_]]()
    var doneSlice = false

    
    def calculate(): Boolean = {
      doneSlice = if (sliceBefore && needsSlice()) {
        completedReduce = nextReduce
        readyNextReduce()
        true
      } else {
        false
      }

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
      if (addedValueToBucket && doneSlice) {
        // we've added a value to a fresh bucket. This won't normally receive this trigger event, as the listener edges are
        // still pending wiring.

        // not sure - is this choice important?
//        env.fireAfterChangingListeners(nextReduce)
        env.wakeupThisCycle(nextReduce)
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

  // wire up slice listening:
  if (sliceEvents != null) {
    if (sliceBefore) env.addListener(sliceEvents, joinValueRendezvous)
    env.addListener(sliceEvents, this)
  }

  // not 100% sure about this - if we are only emitting completed buckets, we close and emit a bucket when the system finishes
  private val termination = env.getTerminationEvent
  if (emitType == ReduceType.LAST) {
    if (sliceBefore) env.addListener(termination, joinValueRendezvous)
    env.addListener(termination, this)
  }

  private def readyNextReduce() {
    if (nextReduce != null) {
      env.removeListener(joinValueRendezvous, nextReduce)
      env.removeListener(nextReduce, this)
      nextReduce.complete()
    }
    nextReduce = newReduce()
    // join values trigger the bucket
    env.addListener(joinValueRendezvous, nextReduce)
    // listen to it so that we propagate value updates to the bucket
    env.addListener(nextReduce, this)
  }
  
  private var nextReduce : Y = _
  readyNextReduce()
  
  private var completedReduce : Y = _

  def value = if (emitType == ReduceType.CUMULATIVE) nextReduce else completedReduce
//  initialised = value != null
  initialised = false // todo: hmm, for CUMULATIVE reduce, do we really think it is worth pushing our state through subsequent map operations?
                      // todo: i.e. by setting initialised == true, we actually fire an event on construction of an empty bucket

  def addInputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
    joinValueRendezvous.addInputBinding(in, adder)
  }

  private class InputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
    def addValueToBucket(bucket:Y) {
      adder(bucket)(in.value)
    }
  }

  def calculate():Boolean = {
    val sliced = if (sliceBefore) {
      joinValueRendezvous.doneSlice
    } else {
      doSliceIfRequired()
    }

    var fire = sliced
    if (emitType == ReduceType.CUMULATIVE && env.hasChanged(value)) fire = true
    if (fire) initialised = true  // belt and braces initialiser
    return fire
  }

  def needsSlice() :Boolean = {
    if (sliceEvents != null && env.hasChanged(sliceEvents)) return true
    if (emitType == ReduceType.LAST && env.hasChanged(termination)) return true
    false
  }

  def doSliceIfRequired() :Boolean = {
    // build a new bucket if necessary
  
    if (needsSlice()) {
      completedReduce = nextReduce
      readyNextReduce()
      true
    } else {
      false
    }
  }
}

