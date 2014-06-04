package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.core._


/**
 * A window-close takes precedence over a new value to be added
 * i.e if the window close event is atomic with a value for the bucket, that value is deemed to be not-in the bucket
 *
 *  event wiring
 *
 * joinInputs--+
 *             |
 *         joinRendezvous -+-> nextReduce -> WindowedBucket_LastValue
 *             |           \                  /
 *             |            +----------------+
 * windowEdge -+----------------------------/
 *
 * todo: remove code duplication with WindowedBucket_Continuous
 *
 *
 * todo: remove code duplication with SlicedBucket. Hang on, is that possible?
 * todo: thinks.... window edges are defined by boolean transitions, therefore I cannot have
 * todo: a window that opens and closes in the same atomic event, which means that 'slice' is impossible.
 * todo: seems so similar in concept that it feels odd to have two different classes.
 * todo: will think more on this.
 */
class WindowedBucket_LastValue[Y <: Bucket](val windowEvents :HasValue[Boolean], cellLifecycle :SliceCellLifecycle[Y], env :types.Env) extends SlicedBucket[Y] {
  private var inWindow = if (windowEvents == null) true else windowEvents.value

  private var nextReduce : Y = _
  private var completedReduce : Y = _

  def value = completedReduce.value
  //  initialised = value != null
  initialised = false // todo: hmm, for CUMULATIVE reduce, do we really think it is worth pushing our state through subsequent map operations?
  // todo: i.e. by setting initialised == true, we actually fire an event on construction of an empty bucket

  // most of the work is actually handled in this 'rendezvous' class
  private val joinValueRendezvous = new types.MFunc {
    var inputBindings = Map[EventGraphObject, InputBinding[_]]()
    var windowEdgeFired = false

    def calculate(): Boolean = {
      windowEdgeFired = false
      var isNowOpen = inWindow
      if (env.hasChanged(windowEvents.getTrigger)) {
        isNowOpen = windowEvents.value
      }
      if (isNowOpen != inWindow) {
        windowEdgeFired = true
        if (isNowOpen) {
          // window started
          inWindow = true
          readyNextReduce()
        } else {
          inWindow = false
          closeCurrentBucket()
        }
      }

      var addedValueToBucket = false
      if (inWindow) { // add some values...
        import collection.JavaConversions.iterableAsScalaIterable
        for (t <- env.getTriggers(this)) {
          val option = inputBindings.get(t)
          if (option.isDefined) {
            option.get.addValueToBucket(nextReduce)
            addedValueToBucket = true
          }
        }
      }
      if (windowEdgeFired && addedValueToBucket) {
        // we've added a value to a fresh bucket. This won't normally receive this trigger event, as the listener edges are
        // still pending wiring.
        // The contract is that a bucket will receive a calculate after it has had its inputs added
        // therefore, we'll send a fire after establishing listener edges to preserve this contract.
        env.fireAfterChangingListeners(nextReduce)
      }

      val fireBucketCell = addedValueToBucket || windowEdgeFired
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

  if (windowEvents != null) {
    env.addListener(windowEvents.getTrigger, joinValueRendezvous)
  }
  env.addListener(joinValueRendezvous, this)
  if (inWindow) {
    readyNextReduce()
  }


  def addInputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
    joinValueRendezvous.addInputBinding(in, adder)
  }

  private class InputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
    def addValueToBucket(bucket:Y) {
      adder(bucket)(in.value)
    }
  }

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

  def calculate():Boolean = {
    if (env.hasChanged(joinValueRendezvous)) {
      // LastValue emit type should only fire on window close
      joinValueRendezvous.windowEdgeFired && !inWindow
    } else {
      false
    }
  }
}

