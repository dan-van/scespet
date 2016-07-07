package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.core.SlicedBucket.JoinValueRendezvous
import scespet.core.SlowGraphWalk.Wakeup
import scespet.util.Logged
import scespet.core.types.MFunc


/**
 * todo: remove code duplication with SlicedReduce
 *
 * if a slice event fires atomically with a value to be added to the bucket:
 * the old bucket is completed and a NEW bucket receives the NEW input value
 *
 *  * i.e. this is 'end-exclusive'

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
class SliceBeforeBucket[S, Y, OUT](cellOut:AggOut[Y,OUT], val sliceSpec :S, cellLifecycle :SliceCellLifecycle[Y], emitType:ReduceType, bindings:List[(HasVal[_], (Y => _ => Unit))], env :types.Env, ev: SliceTriggerSpec[S], exposeInitialValue:Boolean) extends SlicedBucket[Y, OUT] {
  private val cellIsFunction :Boolean = classOf[MFunc].isAssignableFrom( cellLifecycle.C_type.runtimeClass )
  private var nextReduce : Y = _
  var hasExposedCompletedBucket = false
  private var closedReduce : Y = _
  private var hasExposedValueForNextReduce = false
  private var hasExposedValueForClosedReduce = false
  private var bucketHasValue = false
  private var closedBucketHasValue = false
  private var firstBucket = true


  // most of the work is actually handled in this 'rendezvous' class
  private val joinValueRendezvous = new JoinValueRendezvous[Y](this, bindings, env) {
    override def nextReduce: Y = SliceBeforeBucket.this.nextReduce
    var addedValueToBucket = false

    def calculate(): Boolean = {
      val doneSlice = env.hasChanged(sliceHandler)
      if (doneSlice) {
        println("NODEPLOY Is this a fresh bucket?")
      }

      if (hasExposedCompletedBucket) {
        // this covers the case where we have done the bucket-close event, but haven't yet processed the self-wakeup to open a new bucket
        assignNewReduce()
        hasExposedCompletedBucket = false
        ??? // this block should already be covered after recent changes
      }


      import collection.JavaConversions.iterableAsScalaIterable
      if (firstBucket && exposeInitialValue && emitType == ReduceType.CUMULATIVE) {
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

      if (addedValueToBucket && doneSlice && !hasExposedCompletedBucket) {
        // Oh dear - maybe adding to the bucket has just mutated the completed value (i.e. added new data to a sealed bucket)
        // that's a violation
        val updatedSnap = cellOut.out(nextReduce)
        if (updatedSnap == completedReduce) {
          throw new UnsupportedOperationException(s"Danger of double counting in sliceValue: ${completedReduce}. The slice cause is coincident with a value-adding binding, and the value yielded by CellOut is equal to the value snapped before we updated the bucket. This implies that CellOut is generating mutable objects, and it is being mutateg before we can expose it. Please either change the slicing behaviour, or make CellOut generate immutable snaps.")
        }
      }

      val fireBucketCell = addedValueToBucket || doneSlice
      // the rendezvous fires the nextReduce value to notify it of additions
      fireBucketCell
    }
  }

  def assignNewReduce() :Unit = {
    val newCell = cellLifecycle.newCell()
    bucketHasValue = false
    hasExposedValueForNextReduce = false

    // tweak the listeners:
    if (cellIsFunction) {
      if (nextReduce != null && env.hasChanged(nextReduce)) {
        throw new UnsupportedOperationException("We are allocating a new bucket, but that bucket looks like it has just fired, i.e. the bucket generated its own event which is causally 'before' the slice event. This is a requirements contradiction. Either use a SliceAfter, or make the source of events bind to a mutable method on the bucket (which allows us to identify the event source, and ensure that the sice events are ordered after the data events");
      }
      // watch out for the optimisation where the lifecycle re-uses the current cell
      if (newCell != nextReduce) {
          // assigning newReduce is triggered by the slice listener, which comes before the actual SliceBeforeSimpleCell instance.
          // listener removal will be done in the main calculate block so that we can cross-check than no unexpected events have fired
          closedReduce = nextReduce
          nextReduce = newCell

            nextReduce = newCell
          // join values should be come before the reduce can fire
          env.addWakeupOrdering(joinValueRendezvous, nextReduce.asInstanceOf[MFunc])
          // listen to it so that we propagate value updates to the bucket
          env.addListener(nextReduce, this)
      }
    } else {
      nextReduce = newCell
    }
  }
  // init the first reduce
  //    // TODO: if nextReduce was a hasVal, then we'd have strong modelling of initialisation state
  //    env.fireAfterChangingListeners(nextReduce.asInstanceOf[MFunc])
  assignNewReduce()


  val sliceHandler = new MFunc() with Wakeup {
    var fireCount = 0
    override def calculate(): Boolean = {
      if (sliceTriggered()) {
        closeCurrentBucket()
        // we can't open a new bucket just yet, as we need to expose the state of this closed bucket first.
        // nodeploy should we assignNewEduce?
      }
      fireCount += 1
      true
    }
  }
  env.addListener(sliceHandler, joinValueRendezvous)
  // not 100% sure about this - if we are only emitting completed buckets, we close and emit a bucket when the system finishes
  private val termination = env.getTerminationEvent
  if (emitType == ReduceType.LAST) {
    env.addListener(termination, sliceHandler)
  }


  // We can't 'sliceBefore' nextReduce fires, as we have no idea it is about to fire.
  private val eventCountInput = if (cellIsFunction) Set(nextReduce.asInstanceOf[EventGraphObject]) else joinValueRendezvous.inputBindings.keySet
  var sliceEvents :types.EventGraphObject = ev.buildTrigger(sliceSpec, eventCountInput, env)

  // wire up slice listening:
  if (sliceEvents != null) {
    env.addListener(sliceEvents, sliceHandler)
  }

  // NODPLOY maybe 'this' should actually be the JoinValueRendezvous?
  env.addListener(joinValueRendezvous, this)
  if (exposeInitialValue && emitType == ReduceType.CUMULATIVE) env.wakeupThisCycle(joinValueRendezvous)


  private def closeCurrentBucket() {
    if (cellIsFunction && env.hasChanged(nextReduce)) {
      throw new UnsupportedOperationException("Reduce cell fired at the same time as trying to close it")
    }
    cellLifecycle.closeCell(nextReduce)
    completedReduce = cellOut.out(nextReduce)
    closedBucketHasValue = bucketHasValue
    hasExposedValueForClosedReduce = hasExposedValueForNextReduce
    hasExposedValueForNextReduce = false
  }


  private var justClosedBucket = false
  private var completedReduce : OUT = _

  def value:OUT = {
    if (emitType == ReduceType.LAST)
      completedReduce
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
    initialised = exposeInitialValue
  }

  def addInputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
    joinValueRendezvous.addInputBinding(in, adder)
  }

  private class InputBinding[X](in:HasVal[X], adder:Y=>X=>Unit) {
    def addValueToBucket(bucket:Y) {
      adder(bucket)(in.value)
    }
  }

  def calculate():Boolean = {
    val sliceFire = env.hasChanged(sliceHandler)
    val reduceFired = cellIsFunction && env.hasChanged(nextReduce)
    if (sliceFire && reduceFired) {
      // Oh dear - we've had a slice event, but before we've been able to expose the bucket, the bucket itself has also fired.
      // this implies that the bucket is listening to something that is coincident with the slice events, and that the value we
      // snapped may have been mutated from its intended value. check this and alert if necessary
      val updatedSnap = cellOut.out(nextReduce)
      if (updatedSnap == completedReduce) {
        throw new UnsupportedOperationException(s"Danger of double counting in sliceValue: ${completedReduce}." +
          s" The slice cause is coincident with something that the bucket iteself is listening to, and the value yielded " +
          s"by CellOut is equal to the value snapped before we updated the bucket. " +
          s"This implies that CellOut is generating mutable objects, and these are being mutated before we can expose the value." +
          s" Please either change the slicing behaviour, or make CellOut generate immutable snaps.")
      }
    }
    val sliceFired = env.hasChanged(sliceHandler)

    if (hasExposedCompletedBucket && reduceFired) {
      throw new UnsupportedOperationException("The slice was fired, but the bucket fired afterwards before we could call newCell., Can you use sliceAfter, or make the Bucket stop doing its own listener wiring?: "+nextReduce)
    }
    if (hasExposedCompletedBucket) {
      if (sliceFire) {
        throw new AssertionError("Slice has fired again whilst we were trying to process the first one")
      }
      assignNewReduce()
      hasExposedCompletedBucket = false
    }
    if (sliceFire) {
      hasExposedCompletedBucket = true
      env.wakeupThisCycle(this)
    }

    val bucketFire = if (emitType == ReduceType.CUMULATIVE) {
      env.hasChanged(joinValueRendezvous) || reduceFired
    } else {
      false
    }
    // NODEPLOY - in SliceBeforeCell we close the cell here, do we need to call reduce.pause now?
    val cellFired = cellIsFunction && env.hasChanged(nextReduce)
    val addedValueToCell = env.hasChanged(joinValueRendezvous) && joinValueRendezvous.addedValueToBucket

    if (cellFired || addedValueToCell) {
      bucketHasValue = true
    }

    val fire = if (emitType == ReduceType.CUMULATIVE) {
      // in CUMULAIVE mode, only fire an even on a slice fire if the bucket is empty and we're supposed to be exposing empty values
      val newBucketOrBucketClosed = sliceFired || firstBucket
      addedValueToCell || cellFired || (newBucketOrBucketClosed && exposeInitialValue && !bucketHasValue)
    } else {
      // in LAST mode, expose a value on slice.
      sliceFired && (closedBucketHasValue || exposeInitialValue)
    }
    if (fire) {
      firstBucket = false
      initialised = true // belt and braces initialiser // NODEPLOY - is initialised now dead as a concept?
    }
    return fire
  }

  def sliceTriggered() :Boolean = {
    if (sliceEvents != null && env.hasChanged(sliceEvents)) return true
    if (emitType == ReduceType.LAST && env.hasChanged(termination)) return true
    false
  }
}

