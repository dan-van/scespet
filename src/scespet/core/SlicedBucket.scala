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
 *               |
 * sliceEvent ---
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

  val sliceListener = if (sliceBefore) joinValueRendezvous else this
  if (sliceEvents != null) env.addListener(sliceEvents, sliceListener)

  // not 100% sure about this - if we are only emitting completed buckets, we close and emit a bucket when the system finishes
  private val termination = env.getTerminationEvent
  if (emitType == ReduceType.LAST) {
    env.addListener(termination, sliceListener)
  }

  private val joinValueRendezvous = new types.MFunc {
    var inputBindings = Map[EventGraphObject, InputBinding[_]]()
    
    def calculate(): Boolean = {
      val sliced = if (sliceBefore) possiblySlice()
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

    def addInputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) :Boolean = {
      val inputBinding = new InputBinding[X](in, adder)
      val trigger = in.getTrigger
      inputBindings += trigger -> inputBinding
      env.addListener(trigger, this)
      if (in.initialised) {
        inputBinding.addValueToBucket(nextReduce)
        true
      } else {
        false
      }
    }
  }
  
  private def readyNextReduce() {
    if (nextReduce != null) {
      env.removeListener(joinValueRendezvous, nextReduce)
      env.removeListener(nextReduce, this)
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

  def addInputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) :Boolean = {
    joinValueRendezvous.addInputBinding(in, adder)
  }

  private class InputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
    def addValueToBucket(bucket:Y) {
      adder(bucket)(in.value)
    }
  }

  def calculate():Boolean = {
    val sliced = if (!sliceBefore) possiblySlice() else false
    val bucketFired = env.hasChanged(nextReduce)
    if (bucketFired && sliceBefore && sliceEvents != null && env.hasChanged(sliceEvents)) {
      throw new UnsupportedOperationException(s"Bucket($nextReduce) fired concurrently with a slice event($sliceEvents), but this is supposed to be a slice_pre bucket.")
    }
    var fire = sliced
    if (bucketFired && emitType == ReduceType.CUMULATIVE) fire = true
    if (fire) initialised = true  // belt and braces initialiser
    return fire
  }

  def possiblySlice() :Boolean = {
    // build a new bucket if necessary
    var doSlice = (sliceEvents != null && env.hasChanged(sliceEvents))
    if (emitType == ReduceType.LAST && env.hasChanged(termination)) doSlice = true
  
    if (doSlice) {
      completedReduce = nextReduce
      readyNextReduce()
    }
    doSlice
  }
}

