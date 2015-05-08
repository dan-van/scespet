package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.core.SlicedBucket.JoinValueRendezvous
import scespet.core.types._


/**
 * A window-close takes precedence over a new value to be added
 * i.e if the window close event is atomic with a value for the bucket, that value is deemed to be not-in the bucket

 * todo: remove code duplication with WindowedBucket_LastValue

 *
 * todo: remove code duplication with SlicedBucket. Hang on, is that possible?
 * todo: thinks.... window edges are defined by boolean transitions, therefore I cannot have
 * todo: a window that opens and closes in the same atomic event, which means that 'slice' is impossible.
 * todo: seems so similar in concept that it feels odd to have two different classes.
 * todo: will think more on this.
 */
class WindowedBucket_Continuous[Y, OUT](cellOut:AggOut[Y,OUT], val windowEvents :HasValue[Boolean], cellLifecycle :SliceCellLifecycle[Y], bindings:List[(HasVal[_], (Y => _ => Unit))], env :types.Env) extends SlicedBucket[Y, OUT] {
  private var inWindow = if (windowEvents == null) true else windowEvents.value

  private val cellIsFunction :Boolean = classOf[MFunc].isAssignableFrom( cellLifecycle.C_type.runtimeClass )
  private var nextReduce : Y = _
  private var completedReduce : Y = _

  var valueSrc:Y = _ // this has to toggle between nextReduce, and completedReduce
  def value : OUT = cellOut.out(valueSrc)
//  initialised = value != null
  initialised = false // todo: hmm, for CUMULATIVE reduce, do we really think it is worth pushing our state through subsequent map operations?
                      // todo: i.e. by setting initialised == true, we actually fire an event on construction of an empty bucket

  // most of the work is actually handled in this 'rendezvous' class
  private val joinValueRendezvous = new JoinValueRendezvous[Y](this, bindings, env) {
    var windowEdgeFired = false

    override def nextReduce: Y = WindowedBucket_Continuous.this.nextReduce

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
        // NODEPLOY I'll need to do initialisation using  JoinValueRendezvous.pendingInitialValue as we do for SlicedBucket
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
        // NODEPLOY not sure this is still true?

        // we've added a value to a fresh bucket. This won't normally receive this trigger event, as the listener edges are
        // still pending wiring.
        // The contract is that a bucket will receive a calculate after it has had its inputs added
        // therefore, we'll send a fire after establishing listener edges to preserve this contract.
        if (cellIsFunction) env.fireAfterChangingListeners(nextReduce.asInstanceOf[MFunc])
      }

      val fireBucketCell = addedValueToBucket || windowEdgeFired
      fireBucketCell
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
      if (cellIsFunction) {
        env.removeListener(joinValueRendezvous, nextReduce.asInstanceOf[MFunc])
        env.removeListener(nextReduce, this)
      }
      cellLifecycle.closeCell(nextReduce)
    }
    completedReduce = nextReduce
    // expose this one
    valueSrc = completedReduce
  }

  // NOTE: closeCurrentBucket should always be called before this!
  private def readyNextReduce() {
    nextReduce = cellLifecycle.newCell()
    if (cellIsFunction) {
      // join values trigger the bucket
      env.addListener(joinValueRendezvous, nextReduce.asInstanceOf[MFunc])
      // listen to it so that we propagate value updates to the bucket
      env.addListener(nextReduce, this)
    }
    // this is the next bucket, expose it
    valueSrc = nextReduce
  }

  def calculate():Boolean = {
  // listeners can see close events and bucket updates
    if (env.hasChanged(joinValueRendezvous)) {
      if (joinValueRendezvous.windowEdgeFired) {
        if (!inWindow) {
          // this is a close event
          return true
        }
      } else {
        // this is a 'value added to bucket' event
        return true
      }
    }
    if (cellIsFunction && env.hasChanged(valueSrc)) {
      // fire value changes
      return true
    }
    false
  }
}

