package scespet

import scespet.core._
import stub.gsa.esg.mekon.core.{Function, EventGraphObject}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scespet.core.types

/**
 * This is an attempt to more closely capture the high-level expression graph
 * TermBuilder effectively compiles an expression into listener dependencies, and then captures that.
 * I think that's too low-level.
 *
 * @version $Id$
 */
package expression {
  abstract class AbsTerm[IN, X](val source:AbsTerm[_, IN]) extends Term[X] {
    if (source != null) source.addChild(this)

    val children = collection.mutable.Buffer[AbsTerm[X, _]]()
    protected def addChild(child:AbsTerm[X, _]) {
      children += child
    }

    /**
     * apply this expression capture to a given input term
     * @param term
     * @return
     */
    def applyTo(term :Term[IN]):Term[X]

    def map[Y](f: (X) => Y): Term[Y] = new MapTerm[X, Y](this, f)

    def filter(accept: (X) => Boolean): Term[X] = new FilterTerm[X](this, accept)
  }

  class CollectingTerm[X : reflect.ClassTag] extends AbsTerm[X, X](source = null) {
    def applyTo(term: Term[X]): Term[X] = term
  }

  class MapTerm[IN, X](source:AbsTerm[_, IN], val mapFunc:(IN)=>X) extends AbsTerm[IN, X](source) {
    def applyTo(term: Term[IN]): Term[X] = {
      term.map(mapFunc)
    }
  }

  class FilterTerm[X](source:AbsTerm[_, X], val accept: (X) => Boolean) extends AbsTerm[X, X](source) {
    def applyTo(term: Term[X]): Term[X] = {
      term.filter(accept)
    }
  }


  // ---- executing term ----
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

  object Test extends App {
    var root = new CollectingTerm[String]()
    var term = root.map(_.length).filter(_ > 3)

    var newRoot = new ExecutingTerm[String]("Hello")
    applyTree[String](root, newRoot)

    def applyTree[X](node:AbsTerm[_, X], copyTerm:Term[X]) {
      type Y = Any
      for (nodeChild <- node.children) {
        val typedNodeChild = nodeChild.asInstanceOf[AbsTerm[X,Y]]
        val copyChild: Term[Y] = typedNodeChild.applyTo(copyTerm)
        applyTree[Y](typedNodeChild, copyChild)
      }
    }
  }
}
