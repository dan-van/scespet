package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.core.SlicedBucket.JoinValueRendezvous
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


  // most of the work is actually handled in this 'rendezvous' class
  private val joinValueRendezvous = new JoinValueRendezvous[Y](this, bindings, env) {
    override def nextReduce: Y = SliceBeforeBucket.this.nextReduce

    def calculate(): Boolean = {
      if (hasExposedCompletedBucket) {
        // this covers the case where we have done the bucket-close event, but haven't yet processed the self-wakeup to open a new bucket
        assignNewReduce()
        hasExposedCompletedBucket = false
      }

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
      val doneSlice = env.hasChanged(sliceHandler)

      if (addedValueToBucket && doneSlice) {
        // oh, this could be nasty - what if we're using a mutable cell, and the 'out' function is exposing mutable state?
        // NODEPLOY, umm, what is sca
        throw new UnsupportedOperationException("We have closed a bucket, but got an event to add to the new one. We can't do that, because we haven't yet propagated the state of the closed bucket. Requirements contradiction for bucket: "+nextReduce)
      }

      val fireBucketCell = addedValueToBucket || doneSlice
      fireBucketCell
    }
  }

  def assignNewReduce() :Unit = {
    val newCell = cellLifecycle.newCell()
    // tweak the listeners:
    if (cellIsFunction) {
      if (nextReduce != null && env.hasChanged(nextReduce.asInstanceOf[EventGraphObject])) {
        throw new UnsupportedOperationException("We are allocating a new bucket, but that bucket looks like it has just fired, i.e. the bucket generated its own event which is causally 'before' the slice event. This is a requirements contradiction. Either use a SliceAfter, or make the source of events bind to a mutable method on the bucket (which allows us to identify the event source, and ensure that the sice events are ordered after the data events");
      }
      // watch out for the optimisation where the lifecycle re-uses the current cell
      if (newCell != nextReduce) {
        if (nextReduce != null) {
          env.removeListener(joinValueRendezvous, nextReduce.asInstanceOf[MFunc])
          // listen to it so that we propagate value updates to the bucket
          env.removeListener(nextReduce.asInstanceOf[EventGraphObject], this)
        }

        nextReduce = newCell
        // join values trigger the bucket
        env.addListener(joinValueRendezvous, nextReduce.asInstanceOf[MFunc])
        // listen to it so that we propagate value updates to the bucket
        env.addListener(nextReduce.asInstanceOf[EventGraphObject], this)
      }
    } else {
      nextReduce = newCell
    }
  }
  // init the first reduce
  //    // TODO: if nextReduce was a hasVal, then we'd have strong modelling of initialisation state
  //    env.fireAfterChangingListeners(nextReduce.asInstanceOf[MFunc])
  assignNewReduce()


  val sliceHandler = new MFunc() {
    override def calculate(): Boolean = {
      if (sliceTriggered()) {
        closeCurrentBucket()
        // we can't open a new bucket just yet, as we need to expose the state of this closed bucket first.
      }
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


  private def closeCurrentBucket() {
    if (cellIsFunction && env.hasChanged(nextReduce.asInstanceOf[EventGraphObject])) {
      throw new UnsupportedOperationException("Reduce cell fired at the same time as trying to close it")
    }
    cellLifecycle.closeCell(nextReduce)
    completedReduce = cellOut.out(nextReduce)
  }


  private var completedReduce : OUT = _

  def value:OUT = {
    if (emitType == ReduceType.CUMULATIVE)
      cellOut.out(nextReduce)
    else
      completedReduce
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
    val reduceFired = cellIsFunction && env.hasChanged(nextReduce.asInstanceOf[EventGraphObject])
    if (sliceFire && reduceFired) {
      throw new UnsupportedOperationException("We are allocating a new bucket, but that bucket looks like it has just fired, i.e. the bucket generated its own event which is causally 'before' the slice event. This is a requirements contradiction. Either use a SliceAfter, or make the source of events bind to a mutable method on the bucket (which allows us to identify the event source, and ensure that the sice events are ordered after the data events");
    }

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

    val fire = bucketFire || sliceFire
    if (fire)
      initialised = true  // belt and braces initialiser
    return fire
  }

  def sliceTriggered() :Boolean = {
    if (sliceEvents != null && env.hasChanged(sliceEvents)) return true
    if (emitType == ReduceType.LAST && env.hasChanged(termination)) return true
    false
  }
}

