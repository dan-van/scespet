package scespet.core

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:36
 * To change this template use File | Settings | File Templates.
 */
class BucketBuilderImpl[X, Y <: Reduce[X]](newBFunc:() => Y, inputTerm:MacroTerm[X], env:types.Env) extends BucketBuilder[X, Y] {

  def window(windowStream: MacroTerm[Boolean]) :MacroTerm[Y] = {
    // two ways of doing this. Use Mekon primitives, or Reactive
    // actually, reactive doesn't have 'who fired' semantics, which means that we can't distinguish between window fire
    // or window and event
//    class WindowFold extends Reduce[(Y,Boolean)] {
//      var lastState:Boolean = false
//      var nextReduce:Y = _
//      def add(valAndWindow: (Y, Boolean)) = {
//        if (valAndWindow._2 && !lastState) {
//          // window started
//          nextReduce = newBFunc.apply()
//        }
//      }
//    }
//    inputTerm.join(windowStream).reduce()
//    val bucketMaintainer = new HasVal[Y] with types.MFunc
return new MacroTerm[Y](env)(new WindowedReduce[X,Y](inputTerm.input, windowStream.input, newBFunc, env))
//    ???
  }

  def each(n: Int):MacroTerm[Y] = {
    val bucketTrigger = new NewBucketTriggerFactory[X, Y] {
      def create(source: HasVal[X], reduce: Y, env:types.Env) = new NthEvent(n, source.trigger, env)
    }
    return buildTermForBucketStream(newBFunc, bucketTrigger)
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

