package scespet.core

import scespet.core.ReduceType


/**
 * todo: remove code duplication with SlicedReduce
 */
class SlicedBucket[Y <: types.MFunc](val sliceEvents :types.EventGraphObject, val sliceBefore:Boolean, newReduce :()=>Y, emitType:ReduceType, env :types.Env) extends UpdatingHasVal[Y] {
  private var inputBindings = List[InputBinding[_]]()
  private var newSliceNextEvent = false

  if (sliceEvents != null) env.addListener(sliceEvents, this)

  private val termination = env.getTerminationEvent
  if (emitType == ReduceType.LAST) {
    env.addListener(termination, this)
  }

  private var nextReduce : Y = newReduce()
  private var completedReduce : Y = _

  def value = if (emitType == ReduceType.CUMULATIVE) nextReduce else completedReduce
//  initialised = value != null
  initialised = false // todo: hmm, for CUMULATIVE reduce, do we really think it is worth pushing our state through subsequent map operations?
                      // todo: i.e. by setting initialised == true, we actually fire an event on construction of an empty bucket

  def addInputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
    val inputBinding = new InputBinding[X](in, adder)
    inputBindings :+= inputBinding
    // we can't apply the value right now, as we're not sure if this is a before or after slice event
    env.addListener(inputBinding, this)
  }

  private class InputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) extends types.MFunc {
    env.addListener(in.getTrigger, this)
    def calculate(): Boolean = {
      true
    }

    def addValueToBucket(bucket:Y) {
      adder(bucket)(in.value)
    }
  }

  def calculate():Boolean = {
    var fire = false
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
      completedReduce = nextReduce
      nextReduce = newReduce()
      newSliceNextEvent = false
      // just sliced, don't slice again!
      sliceTrigger = false
    }
    // hmm, I should probably provide a dumb implementation of this API call in case we have many inputs...
    import collection.JavaConversions.iterableAsScalaIterable
    var addedValueToBucket = false
    for (t <- env.getTriggers(this)) {
      // is match any slower than instanceof ?
//      t match {case bind:InputBinding[_] => {bind.addValueToBucket(nextReduce)}}
      if (t.isInstanceOf[InputBinding[_]]) {
        t.asInstanceOf[InputBinding[_]].addValueToBucket(nextReduce)
        addedValueToBucket = true
      }
    }
    // for 'cumulative' type, fire if we have added a value, and the bucket says its state has changed
    val bucketFire = if (addedValueToBucket) nextReduce.calculate() else false
    if (emitType == ReduceType.CUMULATIVE) fire |= bucketFire
    
    if (!sliceBefore && sliceTrigger) {
      newSliceNextEvent = true
      if (emitType == ReduceType.LAST) {
        completedReduce = nextReduce
        fire = true
      }
    }
    if (fire) initialised = true  // belt and braces
    return fire
  }
}

