package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.core.SlicedBucket
import scespet.core.SlicedBucket.JoinValueRendezvous
import scespet.util.Logged
import scespet.core.types.MFunc


/**
 * todo: remove code duplication with SlicedReduce
 *
 * if a slice event fires atomically with a value to be added to the bucket:
 * the NEW value is added to the OLD bucket, which is then complete. A NEW bucket will be constructed and presented on some later event.
 * i.e. this is 'end-inclusive'
 *
 *
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
  var queuedNewAssignment = false

  // most of the work is actually handled in this 'rendezvous' class
  private val joinValueRendezvous = new JoinValueRendezvous[Y](this, bindings, env) {
    var newBucketBuilt = false
    var addedValueToBucket = false

    override def nextReduce: Y = SliceAfterBucket.this.nextReduce

    def calculate(): Boolean = {
      // hmm, I should probably provide a dumb implementation of this API call in case we have many inputs...
      import collection.JavaConversions.iterableAsScalaIterable

      addedValueToBucket = false

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

      if (cellIsFunction & newBucketBuilt) {
        // wire up the bucket to this rendezvous so that it gets a calculate whenever we have mutated it
        // PONDER - should I have an API to allow deferred event wiring and put this statement in with the newReduce assignment
        env.addListener(this, nextReduce.asInstanceOf[MFunc])
        newBucketBuilt = false
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
          env.removeListener(nextReduce, SliceAfterBucket.this)
          // eventCountInput hasn't yet got its event (from nextReduce). so we can't yet remove the listener
          env.removeListener(nextReduce, eventCountInput)
        }

        nextReduce = newCell

        // we want to ensure that joinValueRendezvous is 'before' nextReduce
        // this ensures that we get a chance to call the bucket adders before a cell gets to have its calculate func called.
        // however the joinValueRendezvous may have just fired along with the bucket slice, that would cause this new reduce
        // to get a load of events that weren't intended for it. Hence we only add an ordering here, then later promote to a full trigger
        env.addWakeupOrdering(joinValueRendezvous, nextReduce.asInstanceOf[MFunc])
        joinValueRendezvous.newBucketBuilt = true

        // listen to it so that we fire value events whenever the nextReduce fires
        env.addListener(nextReduce, SliceAfterBucket.this)
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
      // fire a cyclic call to ensure that the assignment occurs after we have processed this close operation
      //      queuedNewAssignment = true
      //      env.wakeupThisCycle(this)
    }
    awaitingNextEventAfterReset = true
  }

  def calculate():Boolean = {
//    if (queuedNewAssignment) {
//      queuedNewAssignment = false
//      assignNewReduce()
//    }
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

    val hasNoBucketEvents = awaitingNextEventAfterReset // snap it - this field can mutate in resetCurrentReduce
//    if this is the first fire during construction dont reset it?
    val sliceFire = if (sliceTriggered()) {
      if (env.isInitialised(this)) {
        resetCurrentReduce()
      } else {
        logger.info("NODEPLOY: this was the first calc ever, I'm not going to reset the bucket!")
      }
      true
    } else {
      false
    }

    // PONDER: if the slice fires, and calls 'complete' then a new event / state change should maybe be generated
    // PONDER: hmm, maybe some reductions are undefined until complete has been called, in which case, doing a scan would be meaningless
    val fire = if (emitType == ReduceType.CUMULATIVE) bucketFire || (sliceFire && hasNoBucketEvents) else sliceFire
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

