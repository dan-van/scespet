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
 
class SliceAfterBucket[S, Y, OUT](cellOut:AggOut[Y,OUT], val sliceSpec :S, cellLifecycle :SliceCellLifecycle[Y], emitType:ReduceType, bindings:List[(HasVal[_], (Y => _ => Unit))], env :types.Env, ev: SliceTriggerSpec[S], exposeInitialValue:Boolean) extends SlicedBucket[Y, OUT] {
  var awaitingNextEventAfterReset = false   // start as false so that initialisation is looking at nextReduce.value. May need more thought
  var pendingInitialValue = List[HasVal[_]]()

  private val joinValueRendezvous = new types.MFunc {
    var inputBindings = Map[EventGraphObject, InputBinding[_]]()

    def addInputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
      val inputBinding = new InputBinding[X](in, adder)
      val trigger = in.getTrigger
      inputBindings += trigger -> inputBinding
      env.addListener(trigger, this)
      if (env.hasChanged(trigger)) {
        // we missed the event
        // NODEPLOY - what is the best way to respond? We want to take the new value, and propagate on
        env.wakeupThisCycle(this)
      }
      // PONDER: should we add a value into the bucket when we are binding a new HasVal that is already initialised?
      // if we want to do this, we'll have to watch out against double-inserting in case the
      // hasVal has a pending wakeup pushed onto the stack
      // see also SliceBeforeBucket
      if (in.initialised) {
        pendingInitialValue :+= in
        env.wakeupThisCycle(SliceAfterBucket.this)

        //        inputBinding.addValueToBucket(nextReduce)
        // make sure we wake up to consume this
        // I've chosen 'fireAfterListeners' as I'm worried that more input sources may fire, and since we've not expressed causality
        // relationships, we could fire the nextReduce before that is all complete.
        // it also seems right, we establish all listeners on the new binding before it is fired
        // maybe if this needs to change, we should condition on whether this is a new bucket (which would be fireAfterListeners)
        // or if this is an existing bucket (which should be wakeupThisCycle)
// NODEPLOY - why?
//        if (cellIsFunction) {
//          env.fireAfterChangingListeners(nextReduce.asInstanceOf[MFunc])
//        } else {
//          env.fireAfterChangingListeners(SliceAfterBucket.this)
//        }
      }
    }

    bindings.foreach(pair => {
      val (hasVal, adder) = pair
      type IN = Any
      addInputBinding[IN](hasVal.asInstanceOf[HasVal[IN]], adder.asInstanceOf[Y => IN => Unit])
    })

    def calculate(): Boolean = {
      // hmm, I should probably provide a dumb implementation of this API call in case we have many inputs...
      import collection.JavaConversions.iterableAsScalaIterable
      var addedValueToBucket = false

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
      addedValueToBucket
    }

  }
  env.addListener(joinValueRendezvous, this)


  private val cellIsFunction :Boolean = classOf[MFunc].isAssignableFrom( cellLifecycle.C_type.runtimeClass )
  private var nextReduce : Y = _
  private var completedReduceValue : OUT = _

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
        }

        nextReduce = newCell
        // join values trigger the bucket
        env.addListener(joinValueRendezvous, nextReduce.asInstanceOf[MFunc])
        // listen to it so that we propagate value updates to the bucket
        env.addListener(nextReduce, this)
      }
    } else {
      nextReduce = newCell
    }
  }
  // assign our first bucket
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

    val fire = bucketFire || sliceFire
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

