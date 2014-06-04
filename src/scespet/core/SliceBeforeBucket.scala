package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.util.Logged


/**
 * todo: remove code duplication with SlicedReduce
 *
 * if a slice event fires atomically with a value to be added to the bucket:
 * the old bucket is completed and the next bucket receives the new input value
 * NOTE: emityType = CONTINUOUS is not yet supported as it would imply that for a single input value
 * we'd have to both slice the bucket, and emit an event for the input value being added to the new bucket
 * this is obviously a one to many event propagation, which is not natively supported just yet.
 * I don't want to introduce a buffer to handle this as it breaks event atomicity and just causes problems.
 * I have an idea for how to extend the Envrionment API to support the concept, I'll get back to that later.
 *
 *  event wiring
 *
 * joinInputs--+
 *             |
 *         joinRendezvous -+-> nextReduce -> SliceBeforeBucket
 *             |           \                  /
 *             |            +----------------+
 * sliceEvent -+----------------------------/
 *
 *
 */
 
class SliceBeforeBucket[Y <: Bucket](val sliceEvents :types.EventGraphObject, cellLifecycle :SliceCellLifecycle[Y], emitType:ReduceType, env :types.Env) extends SlicedBucket[Y] {
  if (emitType == ReduceType.CUMULATIVE) throw new UnsupportedOperationException("Not yet implemented due to event atomicity concerns. See class docs")
  // most of the work is actually handled in this 'rendezvous' class
  private val joinValueRendezvous = new types.MFunc {
    var inputBindings = Map[EventGraphObject, InputBinding[_]]()
    var doneSlice = false

    def calculate(): Boolean = {
      doneSlice = false
      if (sliceTriggered()) {
        closeCurrentBucket()
        readyNextReduce()
        doneSlice = true
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
      cellLifecycle.closeCell(nextReduce)
    }
    completedReduce = nextReduce
  }

  // NOTE: closeCurrentBucket should always be called before this!
  private def readyNextReduce() {
    nextReduce = cellLifecycle.newCell()
    // join values trigger the bucket
    env.addListener(joinValueRendezvous, nextReduce)
    // listen to it so that we propagate value updates to the bucket
    env.addListener(nextReduce, this)
  }

  private var nextReduce : Y = _
  readyNextReduce()
  
  private var completedReduce : Y = _

  def value:Y#OUT = if (emitType == ReduceType.CUMULATIVE) nextReduce.value else completedReduce.value
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
    val sliceFire = if (joinValueRendezvous.doneSlice) {
      // consumed
      joinValueRendezvous.doneSlice = false
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
}

