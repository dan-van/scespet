package scespet.core

import gsa.esg.mekon.core.{EventGraphObject, Environment}
import scespet.util._

/**
 * Created by danvan on 16/04/2014.
 */
/**
 * Created by danvan on 16/04/2014.
 */
class PartialReduceOrScanVect[K, X, Y <: Agg[X]](val input:VectTerm[K, X], val bucketGen: (K) => Y, reduceType:ReduceType, val env:Environment) {
  private var sliceTrigger :EventGraphObject = _
  private val sliceBefore = true

  def all() :VectTerm[K,Y#OUT] = {
    val cellBuilder = (index:Int, key:K) => new ReduceAllCell[K, X, Y](input.env, input.input, index, key, bucketGen, reduceType)
    return input.newIsomorphicVector(cellBuilder)
  }

  //  def every(macroTerm:MacroTerm[_], reset:SliceAlign = AFTER) : MacroTerm[Y#OUT] = {
  //    every[MacroTerm[_]](macroTerm, reset = AFTER)(new MacroTermSliceTrigger)
  //  }
  //
  def every[S](sliceSpec:S, reset:SliceAlign = AFTER)(implicit ev:SliceTriggerSpec[S]) : VectTerm[K, Y#OUT] = {
    //  def every[S : SliceTriggerSpec](sliceSpec:S, reset:SliceAlign = AFTER) : MacroTerm[Y#OUT] = {
    //    val sliceTrigger = ev.buildTrigger(sliceSpec, input.getTrigger, env)

    val sliceFunc:Int => types.EventGraphObject = index => {
      val sourceVect = input.input
      val cellFiredTrigger: EventGraphObject = sourceVect.getTrigger(index)
      ev.buildTrigger(sliceSpec, cellFiredTrigger, env)
    }
    val chainedVector = new ChainedVector[K, Y#OUT](input.input, env) {
      def newCell(i: Int, key: K): SlicedReduce[X, Y] = {
        val sourcehasVal = getSourceVector.getValueHolder(i).asInstanceOf[HasVal[X]]
        val perCellSliceTrigger :types.EventGraphObject = sliceFunc(i)
        val noArgNewBucketFunc = () => bucketGen(key)
        val sliceBefore = reset == BEFORE
        new SlicedReduce[X,Y](sourcehasVal, perCellSliceTrigger, sliceBefore, noArgNewBucketFunc, reduceType, env)
      }
    }
    return new VectTerm[K,Y#OUT](env)(chainedVector)
  }

  // TODO: add support for windows
  // TODO: add support for SliceDef against other vectors
  // TODO: Should Bucket join go here?
}
