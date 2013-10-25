package scespet.core

import gsa.esg.mekon.core.EventGraphObject

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:36
 * To change this template use File | Settings | File Templates.
 */
class BucketBuilderVectImpl[K, X, Y <: Reduce[X]](newBFuncFromKey:(K) => Y, inputTerm:VectTerm[K, X], emitType:ReduceType,  env:types.Env) extends BucketBuilderVect[K, X, Y] {
  val newBFunc = () => newBFuncFromKey(null.asInstanceOf[K])
  val inputVectorStream: VectorStream[K, X] = inputTerm.input

//  private class HasValueImpl[V](val h:HasValue[V]) extends HasValue[V] {
//    override def value() = h.value
//    override def getTrigger = h.getTrigger
//  }

  def slice_pre(trigger: EventGraphObject):VectTerm[K,Y] = {
    val sliceTrigger = trigger
    val chainedVector = new ChainedVector[K, Y](inputVectorStream, env) {
      def newCell(i: Int, key: K): SlicedReduce[X, Y] = {
        val sourceHasVal = inputVectorStream.getValueHolder(i)
        val noArgNewBucketFunc = () => newBFuncFromKey(key)
        new SlicedReduce[X, Y](sourceHasVal, sliceTrigger, true, noArgNewBucketFunc, emitType, env)
      }
    }
    return new VectTerm[K,Y](env)(chainedVector)
  }

  def slice_post(trigger: EventGraphObject):VectTerm[K,Y] = {
    val sliceTrigger = trigger
    val chainedVector = new ChainedVector[K, Y](inputVectorStream, env) {
      def newCell(i: Int, key: K): SlicedReduce[X, Y] = {
        val sourceHasVal = inputVectorStream.getValueHolder(i)
        new SlicedReduce[X, Y](sourceHasVal, sliceTrigger, false, newBFunc, emitType, env)
      }
    }
    return new VectTerm[K,Y](env)(chainedVector)
  }


  def window(windowStream: MacroTerm[Boolean]) :VectTerm[K,Y] = {
    val chainedVector = new ChainedVector[K, Y](inputVectorStream, env) {
      def newCell(i: Int, key: K): WindowedReduce[X, Y] = {
        val sourceHasVal = inputVectorStream.getValueHolder(i)
        new WindowedReduce[X,Y](sourceHasVal, windowStream.input, newBFunc, emitType, env)
      }
    }
    return new VectTerm[K,Y](env)(chainedVector)
  }

  def window(windowFunc: K=>HasValue[Boolean]) :VectTerm[K,Y] = {
    val chainedVector = new ChainedVector[K, Y](inputVectorStream, env) {
      def newCell(i: Int, key: K): WindowedReduce[X, Y] = {
        val sourcehasVal = inputVectorStream.getValueHolder(i)
        val perCellWindow = windowFunc(key)
        new WindowedReduce[X,Y](sourcehasVal, perCellWindow, newBFunc, emitType, env)
      }
    }
    return new VectTerm[K,Y](env)(chainedVector)
  }

  /**
   * do a takef on the given vector to get hasValue[Boolean] for each key in this vector.
   * if the other vector does not have the given key, the window will be assumed to be false (i.e. not open)
   * @return
   */
  def window(windowVect: VectTerm[K, Boolean]): VectTerm[K, Y] = {
    val chainedVector = new ChainedVector[K, Y](inputVectorStream, env) {
      def newCell(i: Int, key: K): WindowedReduce[X, Y] = {
        val sourcehasVal = inputVectorStream.getValueHolder(i)
        val idx = windowVect.input.indexOf(key)
        val perCellWindow = windowVect.input.getValueHolder(idx)
        new WindowedReduce[X,Y](sourcehasVal, perCellWindow, newBFunc, emitType, env)
      }
    }
    return new VectTerm[K,Y](env)(chainedVector)
  }

  def each(n: Int):VectTerm[K, Y] = {
    val sliceFunc:Int => types.EventGraphObject = index => {
      val sourceVect = inputTerm.input
      val cellFiredTrigger: EventGraphObject = sourceVect.getTrigger(index)
      new NthEvent(n, cellFiredTrigger, env)
    }
    val chainedVector = new ChainedVector[K, Y](inputVectorStream, env) {
      def newCell(i: Int, key: K): SlicedReduce[X, Y] = {
        val sourcehasVal = inputVectorStream.getValueHolder(i)
        val perCellSliceTrigger :types.EventGraphObject = sliceFunc(i)
        new SlicedReduce[X,Y](sourcehasVal, perCellSliceTrigger, false, newBFunc, emitType, env)
      }
    }
    return new VectTerm[K,Y](env)(chainedVector)
//    val bucketTrigger = new NewBucketTriggerFactory[X, Y] {
//      def create(source: HasVal[X], reduce: Y, env:types.Env) = new NthEvent(n, source.trigger, env)
//    }
//    return buildTermForBucketStream(newBFunc, bucketTrigger)
  }

//  def buildTermForBucketStream[Y <: Reduce[X]](newBFunc:() => Y, triggerBuilder: NewBucketTriggerFactory[X, Y]):MacroTerm[Y] = {
//    // todo: make Window a listenable Func
//    // "start new reduce" is a pulse, which is triggered from time, input.trigger, or Y
//    val input = inputTerm.input
//    val listener = new BucketMaintainer[Y,X](input, newBFunc, triggerBuilder, env)
//    eval.bind(input.trigger, listener)
//    return new MacroTerm[Y](env)(listener)
//  }
}



