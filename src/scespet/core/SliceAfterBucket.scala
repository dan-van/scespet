package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.core.SlicedBucket.JoinValueRendezvous
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
 
class SliceAfterBucket[S, Y, OUT](cellOut:AggOut[Y,OUT], val sliceSpec :S, cellLifecycle :SliceCellLifecycle[Y], emitType:ReduceType, bindings:List[(HasVal[_], (Y => _ => Unit))], env :types.Env, ev: SliceTriggerSpec[S], exposeInitialValue:Boolean) extends SlicedBucket[Y, OUT] {
  private val cellIsFunction :Boolean = classOf[MFunc].isAssignableFrom( cellLifecycle.C_type.runtimeClass )
  private var nextReduce : Y = _
  private var completedReduceValue : OUT = _

  var awaitingNextEventAfterReset = false   // start as false so that initialisation is looking at nextReduce.value. May need more thought

  // most of the work is actually handled in this 'rendezvous' class
  private val joinValueRendezvous = new JoinValueRendezvous[Y](this, bindings, env) {
    var doneSlice = false
    var addedValueToBucket = false

    override def nextReduce: Y = SliceAfterBucket.this.nextReduce

    def calculate(): Boolean = {
      // hmm, I should probably provide a dumb implementation of this API call in case we have many inputs...
      import collection.JavaConversions.iterableAsScalaIterable

      if (pendingInitialValue.nonEmpty) {
        for (in <- pendingInitialValue) {
          if (!env.hasChanged(in.getTrigger)) {
            // this has not fired, but is initialised, so we need to insert the value
            val option = inputBindings.get(in.getTrigger)
            if (option.isDefined) {
              option.get.addValueToBucket(nextReduce)
              addedValueToBucket = true
            }
          }
        }
        pendingInitialValue = List()
      }


      for (t <- env.getTriggers(this)) {
        val option = inputBindings.get(t)
        if (option.isDefined) {
          option.get.addValueToBucket(nextReduce)
          addedValueToBucket = true
        }
      }

      if (cellIsFunction & doneSlice) {
        // wire up the bucket to this rendezvous so that it gets a calculate whenever we have mutated it
        // PONDER - should I have an API to allow deferred event wiring and put this statement in with the newReduce assignment
        env.addListener(this, nextReduce.asInstanceOf[MFunc])
        doneSlice = false
      }

      // whenever we add a value to the bucket, we need to fire, this is because the bucket and/or the SliceAfterBucket need to know when there has been a mutation
      addedValueToBucket
    }

    override def toString: String = "JoinRendezvous{"+SliceAfterBucket.this+"}"
  }
  env.addListener(joinValueRendezvous, this)

  private val eventCountInput = if (!cellIsFunction) joinValueRendezvous else new MFunc() {
    env.addListener(joinValueRendezvous, this)
    override def calculate(): Boolean = true
  }


  def assignNewReduce() :Unit = {
    val newCell = cellLifecycle.newCell()
    // tweak the listeners:
    if (cellIsFunction) {
      // watch out for the optimisation where the lifecycle re-uses the current cell
      if (newCell != nextReduce) {
        if (nextReduce != null) {
          env.removeListener(joinValueRendezvous, nextReduce.asInstanceOf[MFunc])
          // listen to it so that we propagate value updates to the bucket
          env.removeListener(nextReduce, this)
          env.removeListener(nextReduce, eventCountInput)
        }

        nextReduce = newCell
        // input bindings that mutate the nextReduce should fire the bucket.
        // however, don't add the listener just yet, otherwise the new bucket could pick up
        // an event from joinValueRendezvous. Wait until joinValueRendezvous fires.
        // PONDER - should I have an API to allow deferred event wiring?
        joinValueRendezvous.doneSlice = true

        // listen to it so that we fire value events whenever the nextReduce fires
        env.addListener(nextReduce, this)
        env.addListener(nextReduce, eventCountInput)
      }
    } else {
      nextReduce = newCell
    }
  }
  // init the first reduce
  //    // TODO: if nextReduce was a hasVal, then we'd have strong modelling of initialisation state
  //    env.fireAfterChangingListeners(nextReduce.asInstanceOf[MFunc])
  assignNewReduce()


  // if awaitingNextEventAfterReset then the nextReduce has been reset, and we should be exposing the last snap (even if we're in CUMULATIVE mode)
  def value :OUT = if (emitType == ReduceType.LAST || awaitingNextEventAfterReset) completedReduceValue else cellOut.out(nextReduce)

  if (emitType == ReduceType.LAST) {
    initialised = false
  } else {
    // hmm, interesting implications here.
    // a CUMULATIVE reduce will be pushing out state changes for each new datapoint.
    // the question is, is the state valid for downstream map/filter/join before that first datapoint has arrived?
    // i.e. are we happy exposing the emptystate of nextReduce?
    // maybe this answer is up to the implementation of Y?

    // NODEPLOY think about this further, but I'm going with nextReduce is in valid state now.
    // todo: maybe we could tweak this if Y instanceof something with initialisation state?
    // e.g. if nextReduce is a hasVal, should we try to use its initialisation state?
    // alternatively, this could be a question for the cellLifecycle?
    initialised = exposeInitialValue
  }

  def addInputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
    joinValueRendezvous.addInputBinding(in, adder)
  }

  var sliceEvents :types.EventGraphObject = ev.buildTrigger(sliceSpec, Set(eventCountInput), env)
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

  private def resetCurrentReduce() {
    if (nextReduce != null) {
      cellLifecycle.closeCell(nextReduce)
      completedReduceValue = cellOut.out(nextReduce)
      assignNewReduce()
    }
    awaitingNextEventAfterReset = true
  }

  def calculate():Boolean = {
    val bucketFire = if (emitType == ReduceType.CUMULATIVE) {
      if (cellIsFunction) env.hasChanged(nextReduce) else env.hasChanged(joinValueRendezvous)
    } else {
      false
    }
    if (bucketFire) {
      // for a cumulative reduce (i.e. scan), after a bucket reset we need to wait for the next event entering the bucket until we expose the
      // contents of the new bucket.
      // this boolean achieves that
      awaitingNextEventAfterReset = false
    }

    val sliceFire = if (sliceTriggered()) {
      resetCurrentReduce()
      true
    } else {
      false
    }

    val fire = if (emitType == ReduceType.CUMULATIVE) bucketFire else sliceFire
    if (fire) {
      initialised = true // belt and braces initialiser
    }
    return fire
  }

  def sliceTriggered() :Boolean = {
    if (sliceEvents != null && env.hasChanged(sliceEvents)) return true
    if (emitType == ReduceType.LAST && env.hasChanged(termination)) return true
    false
  }
}

