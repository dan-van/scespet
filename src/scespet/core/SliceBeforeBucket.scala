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
  if (emitType == ReduceType.CUMULATIVE) throw new UnsupportedOperationException("Not yet implemented due to event atomicity concerns. See class docs")

  private val cellIsFunction :Boolean = classOf[MFunc].isAssignableFrom( cellLifecycle.C_type.runtimeClass )
  private var nextReduce : Y = _

  // most of the work is actually handled in this 'rendezvous' class
  private val joinValueRendezvous = new JoinValueRendezvous[Y](this, bindings, env) {
    var doneSlice = false

    override def nextReduce: Y = SliceBeforeBucket.this.nextReduce

    def calculate(): Boolean = {
      doneSlice = false
      if (sliceTriggered()) {
        closeCurrentBucket()
        assignNewReduce()
        doneSlice = true
      }

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
      if (cellIsFunction & doneSlice && addedValueToBucket) {
        // we've added a value to a fresh bucket. This won't normally receive this trigger event, as the listener edges are
        // still pending wiring.
        // The contract is that a bucket will receive a calculate after it has had its inputs added
        // therefore, we'll send a fire after establishing listener edges to preserve this contract.
        env.fireAfterChangingListeners(nextReduce.asInstanceOf[MFunc])
      }

      val fireBucketCell = addedValueToBucket || doneSlice
      fireBucketCell
    }
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
  // init the first reduce
  //    // TODO: if nextReduce was a hasVal, then we'd have strong modelling of initialisation state
  //    env.fireAfterChangingListeners(nextReduce.asInstanceOf[MFunc])
  assignNewReduce()


  // We can't 'sliceBefore' nextReduce fires, as we have no idea it is about to fire.
  private val eventCountInput = if (cellIsFunction) Set(nextReduce.asInstanceOf[EventGraphObject]) else joinValueRendezvous.inputBindings.keySet
  var sliceEvents :types.EventGraphObject = ev.buildTrigger(sliceSpec, eventCountInput, env)
  // wire up slice listening:
  if (sliceEvents != null) {
    env.addListener(sliceEvents, joinValueRendezvous)
  }

  // not 100% sure about this - if we are only emitting completed buckets, we close and emit a bucket when the system finishes
  private val termination = env.getTerminationEvent
  if (emitType == ReduceType.LAST) {
    env.addListener(termination, joinValueRendezvous)
  }

  env.addListener(joinValueRendezvous, this)


  private def closeCurrentBucket() {
    if (cellIsFunction && env.hasChanged(nextReduce)) {
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
    val bucketFire = if (emitType == ReduceType.CUMULATIVE) {
      env.hasChanged(value)
    } else {
      false
    }
    val sliceFire = if (joinValueRendezvous.doneSlice) {
      // consumed
      joinValueRendezvous.doneSlice = false
      true
    } else false

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

