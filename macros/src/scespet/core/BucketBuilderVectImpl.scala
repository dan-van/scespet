package scespet.core

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:36
 * To change this template use File | Settings | File Templates.
 */
class BucketBuilderVectImpl[K, X, Y <: Reduce[X]](newBFunc:() => Y, inputTerm:VectTerm[K, X], eval:FuncCollector) extends BucketBuilderVect[X, K, Y] {
  val inputVectorStream: VectorStream[K, X] = inputTerm.input

  def window(windowStream: MacroTerm[Boolean]) :VectTerm[K,Y] = {
    val chainedVector = new ChainedVector[K, WindowedReduce[X, Y], Y](inputVectorStream, eval.env) {
      def newCell(i: Int, key: K): WindowedReduce[X, Y] = {
        val sourcehasVal = new HasVal[X] {
          def value: X = inputVectorStream.get(i)
          def trigger = inputVectorStream.getTrigger(i)
        }
        new WindowedReduce[X,Y](sourcehasVal, windowStream.input, newBFunc, eval.env)
      }

      def get(i: Int): Y = getAt(i).value
    }
    return new VectTerm[K,Y](eval)(chainedVector)
  }

  def window(windowFunc: K=>HasVal[Boolean]) :VectTerm[K,Y] = {
    val chainedVector = new ChainedVector[K, WindowedReduce[X, Y], Y](inputVectorStream, eval.env) {
      def newCell(i: Int, key: K): WindowedReduce[X, Y] = {
        val sourcehasVal = new HasVal[X] {
          def value: X = inputVectorStream.get(i)
          def trigger = inputVectorStream.getTrigger(i)
        }
        val perCellWindow = windowFunc(key)
        new WindowedReduce[X,Y](sourcehasVal, perCellWindow, newBFunc, eval.env)
      }

      def get(i: Int): Y = getAt(i).value
    }
    return new VectTerm[K,Y](eval)(chainedVector)
  }

  def each(n: Int):VectTerm[K, Y] = {
    ???
//    val bucketTrigger = new NewBucketTriggerFactory[X, Y] {
//      def create(source: HasVal[X], reduce: Y, env:types.Env) = new NthEvent(n, source.trigger, env)
//    }
//    return buildTermForBucketStream(newBFunc, bucketTrigger)
  }

//  def buildTermForBucketStream[Y <: Reduce[X]](newBFunc:() => Y, triggerBuilder: NewBucketTriggerFactory[X, Y]):MacroTerm[Y] = {
//    // todo: make Window a listenable Func
//    // "start new reduce" is a pulse, which is triggered from time, input.trigger, or Y
//    val input = inputTerm.input
//    val listener = new BucketMaintainer[Y,X](input, newBFunc, triggerBuilder, eval.env)
//    eval.bind(input.trigger, listener)
//    return new MacroTerm[Y](eval)(listener)
//  }
}



