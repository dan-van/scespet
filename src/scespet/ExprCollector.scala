package scespet

import scespet.core._
import gsa.esg.mekon.core.{Function, EventGraphObject}
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scespet.core.types
import scespet.util._


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

  object CollectingTerm {
    def applyTree[X](node:AbsTerm[_, X], copyTerm:Term[X]) {
      type Y = Any
      for (nodeChild <- node.children) {
        val typedNodeChild = nodeChild.asInstanceOf[AbsTerm[X,Y]]
        val copyChild: Term[Y] = typedNodeChild.applyTo(copyTerm)
        applyTree[Y](typedNodeChild, copyChild)
      }
    }
  }

  class CollectingTerm[X] extends AbsTerm[X, X](source = null) {
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

  object Test extends App {
    // termCollector should know the type of its root nodes.
    // this allows specific builders to know if they can build from a given collector
    var root = new CollectingTerm[String]()
    out("is > 3 chars: ") {root.map(_.length).filter(_ > 3)}

//    collector.installTo( new ExprPrinter( x => "Hello") )
//    new ExprPrinter().executeTree("Hello", root)
//    new ExprPrinter().start("Hello").map(_.length)

    new SimpleEvaluator().addExpression(Seq("Hello", "there", "dan"), root).run()

  }
}
