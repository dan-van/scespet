package scespet

import scespet.core._
import reflect.ClassTag
import scespet.expression.{CapturedTerm, RootTerm}
import gsa.esg.mekon.core.EventGraphObject

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 25/04/2013
 * Time: 19:25
 * To change this template use File | Settings | File Templates.
 */
class ExprPrinter() extends Builder {
  def start[X](x: X) :Term[X] = {
    new ExecutingTerm[X](x)
  }

  class StopTerm[X]() extends Term[X] {
    def map[Y](f: (X) => Y): Term[Y] = new StopTerm[Y]

    def filter(accept: (X) => Boolean): Term[X] = this

    def reduce_all[Y <: Reduce[X]](y: Y) = ???

    def fold_all[Y <: Reduce[X]](y: Y) = ???

    def reduce[Y <: Reduce[X]](newBFunc: => Y) = ???

    def fold[Y <: Reduce[X]](newBFunc: => Y) = ???

    def value = ???

    def by[K](f: (X) => K): MultiTerm[K, X] = ???

    def valueSet[Y](expand: (X) => TraversableOnce[Y]): VectTerm[Y, Y] = ???

    /**
     * emit an updated tuples of (this.value, y) when either series fires
     */
    def join[Y](y: MacroTerm[Y]): MacroTerm[(X, Y)] = ???

    /**
     * Sample this series each time {@see y} fires, and emit tuples of (this.value, y)
     */
    def take[Y](y: MacroTerm[Y]): MacroTerm[(X, Y)] = ???

    /**
     * Sample this series each time {@see y} fires, and emit tuples of (this.value, y)
     */
    def sample(evt: EventGraphObject): MacroTerm[X] = ???
  }

  class ExecutingTerm[X](x:X) extends Term[X] {
    def map[Y](f: (X) => Y): Term[Y] = {
      val y = f(x)
      println(s".map($f) = $y")
      new ExecutingTerm[Y](y)
    }

    def filter(accept: (X) => Boolean): Term[X] = {
      val continue = accept(x)
      println(s".filter($accept) = $continue")
      if (continue) {
        new ExecutingTerm[X](x)
      } else {
        println("STOP")
        new StopTerm[X]
      }
    }

    def reduce_all[Y <: Reduce[X]](y: Y) = ???

    def fold_all[Y <: Reduce[X]](y: Y) = ???

    def reduce[Y <: Reduce[X]](newBFunc: => Y) = ???

    def fold[Y <: Reduce[X]](newBFunc: => Y) = ???

    def value = ???

    def by[K](f: (X) => K): MultiTerm[K, X] = ???

    def valueSet[Y](expand: (X) => TraversableOnce[Y]): VectTerm[Y, Y] = ???

    /**
     * emit an updated tuples of (this.value, y) when either series fires
     */
    def join[Y](y: MacroTerm[Y]): MacroTerm[(X, Y)] = ???

    /**
     * Sample this series each time {@see y} fires, and emit tuples of (this.value, y)
     */
    def take[Y](y: MacroTerm[Y]): MacroTerm[(X, Y)] = ???

    /**
     * Sample this series each time {@see y} fires, and emit tuples of (this.value, y)
     */
    def sample(evt: EventGraphObject): MacroTerm[X] = ???
  }

}
