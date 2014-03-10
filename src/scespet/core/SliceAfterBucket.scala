package scespet.core

import scespet.core.MultiVectorJoin.BucketCell
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
 
class SliceAfterBucket[Y <: Bucket](val sliceEvents :types.EventGraphObject, newReduce :()=>Y, emitType:ReduceType, env :types.Env) extends SlicedBucket[Y] {
  // most of the work is actually handled in this 'rendezvous' class
  private val joinValueRendezvous = new types.MFunc {
    var inputBindings = Map[EventGraphObject, InputBinding[_]]()
    var doneSlice = false
    var sliceNextEvent = false

    def calculate(): Boolean = {
      doneSlice = false
      // slice now if we're in sliceBefore mode and the slice event has fired
      if (sliceNextEvent) {
        readyNextReduce()
        doneSlice = true
        sliceNextEvent = false
      }

      if (sliceTriggered()) {
        sliceNextEvent = true
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
      if (doneSlice && addedValueToBucket) {
        // we've added a value to a fresh bucket. This won't normally receive this trigger event, as the listener edges are
        // still pending wiring.
        // The contract is that a bucket will receive a calculate after it has had its inputs added
        // therefore, we'll send a fire after establishing listener edges to preserve this contract.
        env.fireAfterChangingListeners(nextReduce)
      }
      
      val fireBucketCell = addedValueToBucket || doneSlice
      // need to wake up the slicer to do the slice.
      if (sliceNextEvent) env.wakeupThisCycle(SliceAfterBucket.this)
      fireBucketCell
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
    env.addListener(sliceEvents, joinValueRendezvous)
  }

  // not 100% sure about this - if we are only emitting completed buckets, we close and emit a bucket when the system finishes
  private val termination = env.getTerminationEvent
  if (emitType == ReduceType.LAST) {
    env.addListener(termination, joinValueRendezvous)
  }

  env.addListener(joinValueRendezvous, this)


  private def closeCurrentBucket() {
    if (nextReduce != null) {
      env.removeListener(joinValueRendezvous, nextReduce)
      env.removeListener(nextReduce, this)
      nextReduce.complete()
    }
    completedReduce = nextReduce
  }

  // NOTE: closeCurrentBucket should always be called before this!
  private def readyNextReduce() {
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
    val bucketFire = if (emitType == ReduceType.CUMULATIVE) {
      env.hasChanged(value)
    } else {
      false
    }
    val sliceFire = if (joinValueRendezvous.sliceNextEvent) {
        // this means that we're in slicePost mode, a slice event was fired, all data is added to the bucket, and it is now time to close it.
        closeCurrentBucket()
        true
      } else false

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

  /**
   * allows an external actor to force this bucket builder to seal off the current bucket and use a new one next event.
   */
  def setNewSliceNextEvent() {
    joinValueRendezvous.sliceNextEvent = true
  }
}

