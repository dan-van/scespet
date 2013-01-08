package typetests
import typetests.Chaining.{_}
import scala.reflect.runtime.{universe => ru}
import reflect.ClassTag
import scespet.core._
import reflect.macros.Context
import dan.VectorStream
import typetests.BucketMacro
import stub.gsa.esg.mekon.core.{Environment, EventGraphObject}

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 19/12/2012
 * Time: 21:22
 * To change this template use File | Settings | File Templates.
 */

class SimpleChainImpl[T](val inputData:Seq[T]) extends Term[T]{
  val eventSource = new IteratorEvents[T](inputData)
  val eval: SimpleEvaluator = new SimpleEvaluator
  eval.eventSource = eventSource

  val initialTerm = new MacroTerm[T](eval)(eventSource)


  override def map[Y](f: (T) => Y) = initialTerm.map(f)
  override def by[K](f: (T) => K) = initialTerm.by(f)
  override def map[Y <: Reduce[T]](y: Y) = initialTerm.map(y)
  override def bucket[Y <: Reduce[T]](y: Y, window: Window) = initialTerm.bucket(y, window)

  def run() {
    eval.run()
  }
}

class MacroTerm[X](val eval:FuncCollector)(val input:HasVal[X]) extends Term[X]{
  import scala.reflect.macros.Context
  import scala.language.experimental.macros

  override def map[Y <: Reduce[X]](y: Y):Term[Y] = {
    val listener = new AbsFunc[X,Y] {
      value = y
      def calculate() = {
        y.add(input.value);
        true
      }
    }
    eval.bind(input.trigger, listener)
    return new MacroTerm[Y](eval)(listener)
  }

  override def map[Y](f: (X) => Y):Term[Y] = {
    val listener = new AbsFunc[X,Y] {
      def calculate() = {
        value = f(input.value);
        true
      }
    }
    eval.bind(input.trigger, listener)
    return new MacroTerm[Y](eval)(listener)
  }
//  override def by[K](f: X => K) : VectTerm[K,X] = macro ByMacro.by[K,X]
  override def by[K](f: X => K) : VectTerm[K,X] = {
    val vFunc: GroupFunc[K, X] = new GroupFunc[K, X](input, f, eval.env)
    eval.bind(input.trigger, vFunc)
    return new MacroVectTerm[K, X](eval)(vFunc)
}

  override def bucket[Y <: Reduce[X]](bucketFunc: Y, window: Window):Term[Y] = macro BucketMacro.bucket[X,Y]

  override def bucket2[Y <: Reduce[X]](bucketFunc: Y):BucketBuilder[Term[Y]] = ???

  def bucket2NoMacro[Y <: Reduce[X]](newBFunc:() => Y):BucketBuilder[Term[Y]] = new BucketBuilderImpl[Y](newBFunc)

  class BucketBuilderImpl[Y <: Reduce[X]](newBFunc:() => Y) extends BucketBuilder[Term[Y]] {
    def each(n: Int):Term[Y] = {
      val bucketTrigger = new NewBucketTriggerFactory[X, Y] {
        def create(source: HasVal[X], reduce: Y, env:Environment) = new NthEvent(n, source.trigger, env)
      }
      return bucketNoMacro(newBFunc, bucketTrigger)
    }
  }

  def bucketNoMacro[Y <: Reduce[X]](newBFunc:() => Y, triggerBuilder: NewBucketTriggerFactory[X, Y]):Term[Y] = {
    // todo: make Window a listenable Func
    // "start new bucket" is a pulse, which is triggered from time, input.trigger, or Y
    val listener = new BucketMaintainer[Y,X](input, newBFunc, triggerBuilder, eval.env)
    eval.bind(input.trigger, listener)
    return new MacroTerm[Y](eval)(listener)
  }



}

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

object BucketMacro {
  def bucket[X,Y]
  (c: Context)
  (bucketFunc: c.Expr[Y], window: c.Expr[Window]): c.Expr[Term[Y]] = {
    import c.universe._
    var newBucketFunc = reify(() => {
      val newBucket = bucketFunc.splice; println("constructed new bucket: " + newBucket); newBucket
    })
    // TODO: an impl that takes ()=>Bucket
    ???
  }
}
