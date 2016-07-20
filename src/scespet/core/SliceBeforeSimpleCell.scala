package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.core.SliceBeforeBucket
import scespet.core.SliceCellLifecycle.CellSliceCellLifecycle
import scespet.core.SlicedBucket.JoinValueRendezvous
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
class SliceBeforeSimpleCell[S, Y, OUT](cellOut:AggOut[Y,OUT], val sliceSpec :S, cellLifecycle :CellSliceCellLifecycle[Y], emitType:ReduceType, bindings:List[(HasVal[_], (Y => _ => Unit))], env :types.Env, ev: SliceTriggerSpec[S], exposeEmptyValues:Boolean) extends SlicedBucket[Y, OUT] {
  private val cellIsFunction :Boolean = classOf[MFunc].isAssignableFrom( cellLifecycle.C_type.runtimeClass )
  private var nextReduce : Y = _
  private var closedReduce : Y = _
  private var hasExposedValueForNextReduce = false
  private var hasExposedValueForClosedReduce = false
  private var bucketHasValue = false
  private var closedBucketHasValue = false
  private var firstBucket = true


  // most of the work is actually handled in this 'rendezvous' class
  private val joinValueRendezvous = new JoinValueRendezvous[Y](this, bindings, env) {
    override def nextReduce: Y = SliceBeforeSimpleCell.this.nextReduce
    var addedValueToBucket = false

    def calculate(): Boolean = {
      import collection.JavaConversions.iterableAsScalaIterable
      if (firstBucket && exposeEmptyValues && emitType == ReduceType.CUMULATIVE) {
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
      val doneSlice = env.hasChanged(sliceHandler)

      if (addedValueToBucket && doneSlice) {
        if (cellIsFunction && env.hasChanged(nextReduce.asInstanceOf[EventGraphObject])) {
          throw new UnsupportedOperationException("Reduce cell fired at the same time as trying to close it")
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
      // assigning newReduce is triggered by the slice listener, which comes before the actual SliceBeforeSimpleCell instance.
      // listener removal will be done in the main calculate block so that we can cross-check than no unexpected events have fired
      closedReduce = nextReduce
      nextReduce = newCell
      // join values should be come before the reduce can fire
      env.addWakeupReceiver(joinValueRendezvous, nextReduce.asInstanceOf[MFunc])
      // listen to it so that we propagate value updates to the bucket
      env.addListener(nextReduce.asInstanceOf[EventGraphObject], this)
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
        // because this deals with CellSliceCellLifecycle it is ok to close a bucket and create the next one.
        // because the bucket is not mutable, we are assured that the snapped copy can be exposed as state while we add new events to the
        // new bucket.
        // I could extend this concept in the future, I could rely on the cellOut function to create an immutable snapshot
        // at which point this would be safe in general
        closeCurrentBucket()
        assignNewReduce()
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

  env.addListener(joinValueRendezvous, this)
  if (exposeEmptyValues && emitType == ReduceType.CUMULATIVE) env.wakeupThisCycle(joinValueRendezvous)


  private def closeCurrentBucket() {
    if (cellIsFunction && env.hasChanged(nextReduce.asInstanceOf[EventGraphObject])) {
      throw new UnsupportedOperationException("Reduce cell " + nextReduce + " fired at the same time as trying to close it")
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
    initialised = exposeEmptyValues
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
    val sliceFired = env.hasChanged(sliceHandler)

    if (closedReduce != null && env.hasChanged(closedReduce.asInstanceOf[EventGraphObject])) {
      val postCloseValue = cellOut.out(closedReduce)
      if (completedReduce == postCloseValue) {
        throw new UnsupportedOperationException("We are allocating a new Reduce, but the old Reduce fired after we tried to snap its value.\n" +
          "PONDER: why do I think that a Reduce that is a Function should stop ticking after the slicer completes it?\n" +
          "If we allow it to continue ticking, then it means we need to be confident that its snapped value (from CellOut) is immutable\n" +
          s"Current snapped value: ${completedReduce}\n" +
          "Since I can't be sure that the equivalence of these two values isn't just down to mutability, I'm saying this is unsupported.\n" +
          "You could try switching to SliceAlign.AFTER to make the slice come after the mutation event?" +
          "Alternatively, if the reducing cell isn't a Function, then it can't raise spurious events");
      } else {
        logger.warning(s"Closed reduce fired after closing. Snapped value at close: ${completedReduce}, ignored value: ${postCloseValue}")
      }
    }
    if (closedReduce != null) {
      env.removeListener(joinValueRendezvous, closedReduce.asInstanceOf[MFunc])
      // listen to it so that we propagate value updates to the bucket
      env.removeListener(closedReduce.asInstanceOf[EventGraphObject], this)
      closedReduce = null.asInstanceOf[Y]
    }


    val cellFired = cellIsFunction && env.hasChanged(nextReduce.asInstanceOf[EventGraphObject])
    val addedValueToCell = env.hasChanged(joinValueRendezvous) && joinValueRendezvous.addedValueToBucket

    if (cellFired || addedValueToCell) {
      bucketHasValue = true
    }

    val fire = if (emitType == ReduceType.CUMULATIVE) {
      // in CUMULAIVE mode, only fire an even on a slice fire if the bucket is empty and we're supposed to be exposing empty values
      val newBucketOrBucketClosed = sliceFired || firstBucket
      addedValueToCell || cellFired || (newBucketOrBucketClosed && exposeEmptyValues && !bucketHasValue)
    } else {
      // in LAST mode, expose a value on slice.
      sliceFired && (closedBucketHasValue || exposeEmptyValues)
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

