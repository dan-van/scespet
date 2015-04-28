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

