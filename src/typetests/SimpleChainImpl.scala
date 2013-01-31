package typetests
import typetests.Chaining.{_}
import scala.reflect.runtime.{universe => ru}
import reflect.ClassTag
import scespet.core._
import reflect.macros.Context
import dan.VectorStream
import stub.gsa.esg.mekon.core.{Environment, EventGraphObject}

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 19/12/2012
 * Time: 21:22
 * To change this template use File | Settings | File Templates.
 */

class SimpleChainImpl {
  val eval: SimpleEvaluator = new SimpleEvaluator

  def query[X](inputData:Seq[X]):MacroTerm[X] = {
    val eventSource = new IteratorEvents[X](inputData)
    eval.eventSource = eventSource
    val initialTerm = new MacroTerm[X](eval)(eventSource)
    return initialTerm
  }

  def run() = eval.run()
}


/**
 * This uses a source of data: input, aggregates events into buckets, and provides those buckets as a stream.
 * @param input
 * @param newBFunc
 * @param triggerBuilder
 * @param env
 * @tparam Y
 * @tparam X
 */
class BucketMaintainer[Y <: Reduce[X], X](input:HasVal[X], newBFunc:() => Y, triggerBuilder: NewBucketTriggerFactory[X, Y], env:Environment) extends AbsFunc[X, Y] {
  var nextBucket: Y = _
  var newBucketTrigger: EventGraphObject = null

  def calculate(): Boolean = {
    var closedBucket = false
    if (newBucketTrigger == null || env.hasChanged(newBucketTrigger)) {
      // TODO: distinguish between initial event?
      println(s"Starting new bucket. Old = $nextBucket, bucketTrigger = $newBucketTrigger")
      if (nextBucket != null) {
        value = nextBucket
        closedBucket = true
      }
      nextBucket = newBFunc.apply()
      val newTrigger = triggerBuilder.create(input, value, env)
      if (newTrigger != newBucketTrigger) {
        if (newBucketTrigger != null) {
          env.removeListener(newBucketTrigger, this)
        }
        env.addListener(newTrigger, this)
        newBucketTrigger = newTrigger
      }
    }
    if (env.hasChanged(input.trigger)) {
      nextBucket.add(input.value);
    }
    return closedBucket
  }
}


class MacroVectTerm[K,X](val eval:FuncCollector)(input:VectorStream[K,X]) extends VectTerm[K,X] {

}
class BucketBuilderImpl[X, Y <: Reduce[X]](newBFunc:() => Y, input:HasVal[X], eval:FuncCollector) extends BucketBuilder[MacroTerm[Y]] {
  def each(n: Int):MacroTerm[Y] = {
    val bucketTrigger = new NewBucketTriggerFactory[X, Y] {
      def create(source: HasVal[X], reduce: Y, env:Environment) = new NthEvent(n, source.trigger, env)
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
