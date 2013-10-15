package scespet

import scespet.core._
import gsa.esg.mekon.core.{Function, EventGraphObject}
import scala.collection.mutable.ArrayBuffer
import scespet.core.types


/**
 * This is an attempt to more closely capture the high-level expression graph
 * TermBuilder effectively compiles an expression into listener dependencies, and then captures that.
 * I think that's too low-level.
 *
 * @version $Id$
 */
package expression {

import java.util.logging.Level
import scala.collection.mutable

class Scesspet {
    val leaves = collection.mutable.Set[AbsTerm[_,_]]()
    val allNodes = collection.mutable.Set[AbsTerm[_,_]]()

    def query[X](data: TraversableOnce[X], timeFunc:(X, Int)=>Long) : Term[X] = {
      var root = new TraversableRoot[X](data, timeFunc)
      addNode(null, root)
      root
    }

    def query[X](newHasVal :(types.Env) => HasVal[X]) : AbsTerm[X, X] = {
      var root = new HasValRoot[X](newHasVal)
      addNode( null, root )
      root
    }

    def addNode(parent:AbsTerm[_,_], child:AbsTerm[_,_]) = {
      if (parent != null) {
        leaves -= parent
        allNodes += parent
      }
      if (! allNodes.contains(child)) {
        leaves += child
        allNodes += child
      }
    }
  }

  abstract class AbsTerm[IN, X](val source:AbsTerm[_, IN]) extends Term[X] with Serializable {
    // are joins multi parent?
    val parent :AbsTerm[_, IN] = source
    if (source != null) {
      source.addChild(this)
    }

    val children = collection.mutable.LinkedHashSet[AbsTerm[X, _]]()
    protected def addChild(child:AbsTerm[X, _]) {
      children += child
    }

    /**
     * apply this expression capture to a given input term
     * @param term
     * @return
     */
    def applyTo(term :Term[IN]):Term[X]


    def value = ???

    def map[Y](f: (X) => Y) = new MapTerm[X, Y](this, f)

    def filter(accept: (X) => Boolean) = new FilterTerm[X](this, accept)

    def reduce_all[Y <: Reduce[X]](y: Y) = reduce(y).all()

    def reduce[Y <: Reduce[X]](newBFunc: => Y) = new CollectCapture[Y, X](false, this, newBFunc)

    def fold[Y <: Reduce[X]](newBFunc: => Y) = new CollectCapture[Y, X](true, this, newBFunc)

    def fold_all[Y <: Reduce[X]](y: Y) = new FoldAllTerm[X,Y](this, y)

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

//  object RootTerm {
//    def applyTree[X](node:AbsTerm[_, X], copyTerm:Term[X]) {
//      type Y = Any
//      for (nodeChild <- node.children) {
//        val typedNodeChild = nodeChild.asInstanceOf[AbsTerm[X,Y]]
//        val copyChild: Term[Y] = typedNodeChild.applyTo(copyTerm)
//        applyTree[Y](typedNodeChild, copyChild)
//      }
//    }
//  }

  class TraversableRoot[X](val data: TraversableOnce[X], timeFunc:(X, Int)=>Long) extends RootTerm[X] {
    def buildHasVal(env: types.Env): HasVal[X] = {
      val events = new IteratorEvents[X](data, timeFunc)
      env.registerEventSource(events)
      events
    }
  }
  class HasValRoot[X](val newHasVal :(types.Env) => HasVal[X]) extends RootTerm[X] {
    def buildHasVal(env: types.Env): HasVal[X] = {
      var events = newHasVal.apply(env)
      env.setStickyInGraph(events.trigger, true)
      events
    }
  }

  abstract class RootTerm[X] extends AbsTerm[X, X](source = null) {
    def applyTo(term: Term[X]): Term[X] = ???
    def buildHasVal(env :types.Env) :HasVal[X]
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

  @deprecated("This should be unnecessary when bucketBuilder supports fold")
  class FoldAllTerm[X, Y <: Reduce[X] ](source:AbsTerm[_, X], val fold: Y) extends AbsTerm[X, Y](source) {
    def applyTo(term: Term[X]): Term[Y] = {
      term.fold_all(fold)
    }
  }


  class CollectTerm[IN, X <: Reduce[IN]](collectCapture:CollectCapture[X, IN]) (bucketBuilderCall:(BucketBuilder[IN, X]) => Term[X]) extends AbsTerm[IN, X](collectCapture.input) {

    def applyTo(term: Term[IN]): Term[X] = {
      if (collectCapture.continuousOutput) {
//        val bucketBuilder = term.fold(collectCapture.reduce)
//        bucketBuilderCall.apply(bucketBuilder)
        ??? //actuall, bucketBuilder is only bound to "reduce" not "fold" right now
      } else {
        val bucketBuilder = term.reduce(collectCapture.reduce)
        bucketBuilderCall.apply(bucketBuilder)
      }
    }
  }

  class CollectCapture[T <: Reduce[X], X](val continuousOutput:Boolean, val input:AbsTerm[_, X], val reduce:T) extends BucketBuilder[X,T] {
    def each(n: Int) = ???

    def window(windowStream: Term[Boolean]) = ???

    def all() = new CollectTerm[X, T](this)( _.all() )

    //
    def slice_pre(trigger: MacroTerm[_]) = ???
    def slice_pre(trigger: EventGraphObject) = ???

    def slice_post(trigger: MacroTerm[_]) = ???
    def slice_post(trigger: EventGraphObject) = ???
  }

}
