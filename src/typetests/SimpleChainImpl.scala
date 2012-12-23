package typetests
import typetests.Chaining.{_}
import scala.reflect.runtime.{universe => ru}
import reflect.ClassTag
import scespet.core._

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

class MacroTerm[X](val eval:FuncCollector)(input:HasVal[X]) extends Term[X]{
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
  override def by[K](f: (X) => K) = ???

  override def bucket[Y <: Reduce[X]](y: Y, window: Window) = ???
}
