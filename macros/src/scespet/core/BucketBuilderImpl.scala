package scespet.core

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:36
 * To change this template use File | Settings | File Templates.
 */
class BucketBuilderImpl[X, Y <: Reduce[X]](newBFunc:() => Y, input:HasVal[X], eval:FuncCollector) extends BucketBuilder[Y] {
  def each(n: Int):MacroTerm[Y] = {
    val bucketTrigger = new NewBucketTriggerFactory[X, Y] {
      def create(source: HasVal[X], reduce: Y, env:types.Env) = new NthEvent(n, source.trigger, env)
    }
    return buildTermForBucketStream(newBFunc, bucketTrigger)
  }

  def buildTermForBucketStream[Y <: Reduce[X]](newBFunc:() => Y, triggerBuilder: NewBucketTriggerFactory[X, Y]):MacroTerm[Y] = {
    // todo: make Window a listenable Func
    // "start new bucket" is a pulse, which is triggered from time, input.trigger, or Y
    val listener = new BucketMaintainer[Y,X](input, newBFunc, triggerBuilder, eval.env)
    eval.bind(input.trigger, listener)
    return new MacroTerm[Y](eval)(listener)
  }
}

class NthEvent(val N:Int, val source:types.EventGraphObject, env:types.Env) extends types.MFunc {
  var n = 0;
  env.addListener(source, this)
  def calculate():Boolean = {n += 1; return n % N == 0}
}

