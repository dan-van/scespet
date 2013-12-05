package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.core.types.MFunc

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:36
 * To change this template use File | Settings | File Templates.
 */
class BucketBuilderImpl[X, Y <: Reduce[X]](newBFunc:() => Y, inputTerm:MacroTerm[X], emitType:ReduceType, env:types.Env) extends BucketBuilder[X, Y] {

  def window(windowStream: Term[Boolean]) :MacroTerm[Y] = {
    val windowHasVal = windowStream.asInstanceOf[MacroTerm[Boolean]].input
    return new MacroTerm[Y](env)(new WindowedReduce[X,Y](inputTerm.input, windowHasVal, newBFunc, emitType, env))
  }

  def all():MacroTerm[Y] = {
    if (emitType == ReduceType.LAST) {
      val bucketTrigger = new NewBucketTriggerFactory[X, Y] {
        def create(source: HasVal[X], reduce: Y, env:types.Env) = new types.MFunc {
          def calculate() = false // never create a new bucket
        }
      }
      return buildTermForBucketStream(newBFunc, bucketTrigger)
    } else {
      // really simple implementation TODO: clean up fold_all and reduce_all
      val inputCell = inputTerm.input
      val outputCell = new UpdatingHasVal[Y] {
        val bucket = newBFunc()
        env.addListener( inputCell.getTrigger, this)

        def calculate() = { bucket.add(inputCell.value); true }
        def value = bucket
      }
      return new MacroTerm[Y](env)(outputCell)
    }
  }

  def each(n: Int):MacroTerm[Y] = {
    val sliceTrigger = new NthEvent(n, inputTerm.input.getTrigger, env)
    val sliceBefore = false // always slice after the event has been added to the bucket
    val slicer = new SlicedReduce[X, Y](inputTerm.input, sliceTrigger, sliceBefore, newBFunc, emitType, env)
    return new MacroTerm[Y](env)(slicer)
  }

  def slice_pre(trigger: MacroTerm[_]):MacroTerm[Y] = {
    slice_pre(trigger.input.trigger)
  }

  def slice_pre(trigger: EventGraphObject):MacroTerm[Y] = {
    val sliceTrigger = trigger
    val slicer = new SlicedReduce[X, Y](inputTerm.input, sliceTrigger, true, newBFunc, emitType, env)
    return new MacroTerm[Y](env)(slicer)
  }

  def slice_post(trigger: MacroTerm[_]):MacroTerm[Y] = {
    slice_post(trigger.input.trigger)
  }

  def slice_post(trigger: EventGraphObject):MacroTerm[Y] = {
    val sliceTrigger = trigger
    val slicer = new SlicedReduce[X, Y](inputTerm.input, sliceTrigger, false, newBFunc, emitType, env)
    return new MacroTerm[Y](env)(slicer)
  }

  def buildTermForBucketStream[Y <: Reduce[X]](newBFunc:() => Y, triggerBuilder: NewBucketTriggerFactory[X, Y]):MacroTerm[Y] = {
    // todo: make Window a listenable Func
    // "start new reduce" is a pulse, which is triggered from time, input.trigger, or Y
    val input = inputTerm.input
    val listener = new BucketMaintainer[Y,X](input, newBFunc, triggerBuilder, env)
    env.addListener(input.trigger, listener)
    return new MacroTerm[Y](env)(listener)
  }
}

class NthEvent(val N:Int, val source:types.EventGraphObject, env:types.Env) extends types.MFunc {
  var n = 0;
  env.addListener(source, this)
  def calculate():Boolean = {n += 1; return n % N == 0}
}

