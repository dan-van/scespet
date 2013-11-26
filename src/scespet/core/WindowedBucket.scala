package scespet.core

import scespet.core.MultiVectorJoin.BucketCell


/**
 * todo: remove code duplication with SlicedBucket. Hang on, is that possible?
 * todo: thinks.... window edges are defined by boolean transitions, therefore I cannot have
 * todo: a window that opens and closes in the same atomic event, which means that 'slice' is impossible.
 * todo: seems so similar in concept that it feels odd to have two different classes.
 * todo: will think more on this.
 */
class WindowedBucket[Y <: Bucket](val windowEvents :HasValue[Boolean], newReduce :()=>Y, emitType:ReduceType, env :types.Env) extends UpdatingHasVal[Y] with BucketCell[Y]{
  private var inputBindings = List[InputBinding[_]]()

  private var inWindow = if (windowEvents == null) true else windowEvents.value
  if (windowEvents != null) env.addListener(windowEvents.getTrigger, this)

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

  def addInputBinding[X](in:HasVal[X], adder:Y=>X=>Unit):Boolean = {
    val inputBinding = new InputBinding[X](in, adder)
    inputBindings :+= inputBinding
    // we can't apply the value right now, as we're not sure if this is a before or after slice event
    env.addListener(inputBinding, this)
    if (in.initialised) {
      inputBinding.addValueToBucket(nextReduce)
      true
    } else {
      false
    }
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
    // build a new bucket if necessary
    var isNowOpen = inWindow
    if (env.hasChanged(windowEvents.getTrigger)) {
      isNowOpen = windowEvents.value
    }
    if (isNowOpen && !inWindow) {
      // window started
      nextReduce = newReduce()
      inWindow = true
    }
    // guess I may need to convert this to Java iterator for performance...
    import collection.JavaConversions.iterableAsScalaIterable
    var addedValueToBucket = false

    // note, if the window close coincides with this event, we currently discard the datapoint
    // I chose this because an event that defines the 'end' of an acceptable period sounds like
    // we don't want the event.
    // however... that explanation sounds dodgy, but do I really want to have
    // 'slice_pre' and 'slice_post' window types? hmmmm, wait for usecase.
    if (isNowOpen) {
      for (t <- env.getTriggers(this)) {
        // is match any slower than instanceof ?
  //      t match {case bind:InputBinding[_] => {bind.addValueToBucket(nextReduce)}}
        if (t.isInstanceOf[InputBinding[_]]) {
          t.asInstanceOf[InputBinding[_]].addValueToBucket(nextReduce)
          addedValueToBucket = true
        }
      }
    }
    // for 'cumulative' type, fire if we have added a value, and the bucket says its state has changed
    val bucketFire = if (addedValueToBucket) nextReduce.event() else false
    if (emitType == ReduceType.CUMULATIVE) fire |= bucketFire

    if (inWindow && !isNowOpen) {
      // window closed. snap the current reduction and get ready for a new one
      completedReduce = nextReduce
      inWindow = false
      fire = true
    }
    if (fire) {
      initialised = true
    }
    return fire
  }
}

