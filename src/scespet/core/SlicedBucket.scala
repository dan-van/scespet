package scespet.core


import gsa.esg.mekon.core.EventGraphObject
import scespet.util.Logged


/**
 * todo: remove code duplication with SlicedReduce
 *  event wiring
 *
 * joinInputs--+
 *             |
 *         joinRendezvous -+-> nextReduce -> SlicedBucket
 *             |           \                  /
 *             |            +----------------+
 * sliceEvent -+----------------------------/
 *
 *
 */
 
abstract class SlicedBucket[C, OUT] extends UpdatingHasVal[OUT] with Logged {
  def addInputBinding[IN](in:HasVal[IN], adder:C=>IN=>Unit)
}

object SlicedBucket {
  class InputBinding[B,X](in:HasVal[X], adder:B=>X=>Unit) {
    def addValueToBucket(bucket:B) {
      adder(bucket)(in.value)
    }
  }

  abstract class JoinValueRendezvous[B](bucketSlicer:SlicedBucket[B, _], bindings:List[(HasVal[_], (B => _ => Unit))], env:types.Env) extends types.MFunc with Logged {
    var inputBindings = Map[EventGraphObject, InputBinding[B, _]]()

    // NOTE: any implemenation of calculate needs to consume the pendingInitialValue set. I may change classes to make this more obvious
    var pendingInitialValue = List[HasVal[_]]()

    def nextReduce:B

    def addInputBinding[X](in:HasVal[X], adder:B=>X=>Unit) {
      val inputBinding = new InputBinding[B, X](in, adder)
      val trigger = in.getTrigger
      inputBindings += trigger -> inputBinding
      env.addListener(trigger, this)
      if (in.initialised != env.hasChanged(in.getTrigger)) {
        logger.warning(s"NODEPLOY I wanted to replace in.initialised (${in.initialised}) with simply env.hasChanged (or maybe env.isInitialised)")
      }
//      if (env.hasChanged(trigger)) {
//        // we missed the event
//        // NODEPLOY - what is the best way to respond? We want to take the new value, and propagate on
//        env.wakeupThisCycle(this)
//      }
      // PONDER: should we add a value into the bucket when we are binding a new HasVal that is already initialised?
      // if we want to do this, we'll have to watch out against double-inserting in case the
      // hasVal has a pending wakeup pushed onto the stack
      // see also SliceBeforeBucket
      if (in.initialised) {
        pendingInitialValue :+= in
        env.wakeupThisCycle(this)

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
      addInputBinding[IN](hasVal.asInstanceOf[HasVal[IN]], adder.asInstanceOf[B => IN => Unit])
    })


  }

}

//object SlicedBucket {
//  def buildWindow[IN, B, OUT](reduceType:ReduceType, cellOut:AggOut[B,OUT], window :HasValue[Boolean], lifecycle :SliceCellLifecycle[B], env:types.Env) :SlicedBucket[B, OUT] = {
//    reduceType match {
//      case ReduceType.CUMULATIVE => new WindowedBucket_Continuous[B, OUT](cellOut, window, lifecycle, env)
//      case ReduceType.LAST => new WindowedBucket_LastValue[B, OUT](cellOut, window, lifecycle, env)
//    }
//  }
//
//  def buildSliced[IN, B, OUT, S](reduceType:ReduceType, cellOut:AggOut[B,OUT], sliceSpec :S, lifecycle :SliceCellLifecycle[B], env:types.Env, ev: SliceTriggerSpec[S]) :SlicedBucket[B, OUT] = {
//???
////      new SlicedReduce[S, IN, A, OUT](input, adder, cellOut, sliceSpec, triggerAlign == BEFORE, lifecycle, reduceType, env, ev, exposeInitialValue = false)
////      val slicer = new SliceBeforeBucket[Any, Y, OUT](cellOut, null, lifecycle, ReduceType.LAST, env, SliceTriggerSpec.TERMINATION, exposeInitialValue = false)
////      val slicer = new SliceAfterBucket[Null, Y, OUT](cellOut, null, lifecycle, ReduceType.CUMULATIVE, env, SliceTriggerSpec.NULL, exposeInitialValue = false)
//
//  }
//}

