package scespet.core

import scespet.core.SlicedBucket.JoinValueRendezvous
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
 
class SliceAfterSimpleCell[S, Y, OUT](cellOut:AggOut[Y,OUT], val sliceSpec :S, cellLifecycle :SliceCellLifecycle[Y], emitType:ReduceType, bindings:List[(HasVal[_], (Y => _ => Unit))], env :types.Env, ev: SliceTriggerSpec[S], exposeEmptyBucket:Boolean) extends SlicedBucket[Y, OUT] {
  private val cellIsFunction :Boolean = classOf[MFunc].isAssignableFrom( cellLifecycle.C_type.runtimeClass )
  private var nextReduce : Y = _
  private var completedReduceValue : OUT = _

  var bucketHasValue = false   // start as false so that initialisation is looking at nextReduce.value. May need more thought
  var queuedNewAssignment = false

  // most of the work is actually handled in this 'rendezvous' class
  private val joinValueRendezvous = new JoinValueRendezvous[Y](this, bindings, env) {
    var addListenerToNewReduce = false
    var addedValueToBucket = false

    override def nextReduce: Y = SliceAfterSimpleCell.this.nextReduce

    def calculate(): Boolean = {
      // hmm, I should probably provide a dumb implementation of this API call in case we have many inputs...
      import collection.JavaConversions.iterableAsScalaIterable

      if (queuedNewAssignment) {
        queuedNewAssignment = false
        assignNewReduce()
        ??? // I think I can delete this path
      }

      if (firstBucket && exposeEmptyBucket && emitType == ReduceType.CUMULATIVE) {
        // we are forced to emit an empty bucket, but we may have inputs that want to feed into the backet.
        // bit of a hack, as I'm not actually capturing the values from the inputs, merely assuming I can apply them in
        // a subsequent cycle for the same effect (after we have exposed the empty state)
        for (bind <- bindings) {
          val bindingInput = bind._1
          if (env.hasChanged(bindingInput.getTrigger)) {
            logger.info("Capturing deferred event from "+bindingInput)
            pendingInitialValue :+= bindingInput
          }
        }
        env.wakeupThisCycle(this)
        return true
      }

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

      if (cellIsFunction & addListenerToNewReduce) {
        // wire up the bucket to this rendezvous so that it is strictly 'after' any mutations that the joinValueRendezvous may apply
        env.addWakeupOrdering(this, nextReduce.asInstanceOf[MFunc])
        addListenerToNewReduce = false
      }

      // whenever we add a value to the bucket, we need to fire, this is because the bucket and/or the SliceAfterBucket need to know when there has been a mutation
      addedValueToBucket
    }

    override def toString: String = "JoinRendezvous{"+SliceAfterSimpleCell.this+"}"
  }
  env.addListener(joinValueRendezvous, this)

  private val eventCountInput = if (!cellIsFunction) joinValueRendezvous else new MFunc() {
    env.addListener(joinValueRendezvous, this)
    override def calculate(): Boolean = true
  }
  env.addWakeupOrdering(eventCountInput, this) // this is so that we know we are deterministically after event count input, and


  def assignNewReduce() :Unit = {
    val newCell = cellLifecycle.newCell()
    bucketHasValue = false

    // tweak the listeners:
    if (cellIsFunction) {
      // watch out for the optimisation where the lifecycle re-uses the current cell
      if (newCell != nextReduce) {
        if (nextReduce != null) {
          env.removeListener(joinValueRendezvous, nextReduce.asInstanceOf[MFunc])
          env.removeWakeupOrdering(joinValueRendezvous, nextReduce.asInstanceOf[MFunc])
          // eventCountInput hasn't yet got its event (from nextReduce). so we can't yet remove the listener
          env.removeListener(nextReduce, eventCountInput)
          // listen to it so that we propagate value updates to the bucket
          env.removeListener(nextReduce, SliceAfterSimpleCell.this)
        }

        nextReduce = newCell

        // we want to ensure that joinValueRendezvous is 'before' nextReduce
        // this ensures that we get a chance to call the bucket adders before a cell gets to have its calculate func called.
        // however the joinValueRendezvous may have just fired along with the bucket slice, that would cause this new reduce
        // to get a load of events that weren't intended for it. Hence we only add an ordering here, then later promote to a full trigger
        env.addWakeupOrdering(joinValueRendezvous, nextReduce.asInstanceOf[MFunc])
        joinValueRendezvous.addListenerToNewReduce = true

        // listen to it so that we fire value events whenever the nextReduce fires
        env.addListener(nextReduce, SliceAfterSimpleCell.this)
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
  var firstBucket = true
  // in 'exposeEmptyBucket' mode, when running CUMULATIVE, we should expose the initial empty bucket for completeness
  if (exposeEmptyBucket && emitType == ReduceType.CUMULATIVE) env.wakeupThisCycle(joinValueRendezvous)


  def value:OUT = {
    if (emitType == ReduceType.LAST)
      completedReduceValue
    else
      cellOut.out(nextReduce)
  }

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
    initialised = exposeEmptyBucket
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

  def calculate():Boolean = {
    val newBucketCreated = if (queuedNewAssignment) {
      queuedNewAssignment = false
      assignNewReduce()
      true
    } else {
      firstBucket
    }
    firstBucket = false

    val bucketChange = env.hasChanged(joinValueRendezvous) && joinValueRendezvous.addedValueToBucket || cellIsFunction && env.hasChanged(nextReduce)
    if (bucketChange) {
      // for a cumulative reduce (i.e. scan), after a bucket reset we need to wait for the next event entering the bucket until we expose the
      // contents of the new bucket.
      // this boolean achieves that
      bucketHasValue = true
    }

//    if this is the first fire during construction dont reset it?
    val sliceFire = if (sliceTriggered()) {
      if (env.isInitialised(this)) {
        if (nextReduce != null) {
          cellLifecycle.closeCell(nextReduce)
          completedReduceValue = cellOut.out(nextReduce)
          // we need to propagate the fire for the current bucket, before we can put a new one in place.
          queuedNewAssignment = true
          env.wakeupThisCycle(this) // queue up a cyclic fire
        }
      } else {
        logger.info("NODEPLOY: this was the first calc ever, I'm not going to reset the bucket!")
      }
      true
    } else {
      false
    }

    val fire = if (emitType == ReduceType.CUMULATIVE) {
      if (newBucketCreated && exposeEmptyBucket && bucketChange) {
          throw new UnsupportedOperationException("I can't expose an empty bucket, as the bucket itself has fired an event at construction. I don't know if this inconsistency is bad semantics, ot simply irrelevant?")
      }
      bucketChange || (exposeEmptyBucket && newBucketCreated)
    } else {
      sliceFire && (bucketHasValue || exposeEmptyBucket)
    }
    if (fire) {
      initialised = true // belt and braces initialiser
//      firedForBucket = true
    }
    return fire
  }

  def sliceTriggered() :Boolean = {
    if (sliceEvents != null && env.hasChanged(sliceEvents)) return true
    if (emitType == ReduceType.LAST && env.hasChanged(termination)) return true
    false
  }
}

