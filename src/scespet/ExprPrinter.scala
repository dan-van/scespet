package scespet

import core.Term
import reflect.ClassTag
import scespet.expression.{AbsTerm, CollectingTerm}

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

  def executeTree[X](startVal: X, expressionRoot: CollectingTerm[X]) = {
    var exec = new ExecutingTerm[X](startVal)
    CollectingTerm.applyTree(expressionRoot, exec)
  }

  class StopTerm[X]() extends Term[X] {
    def map[Y](f: (X) => Y): Term[Y] = new StopTerm[Y]

    def filter(accept: (X) => Boolean): Term[X] = this
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
  }

}
