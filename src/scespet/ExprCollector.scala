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

import gsa.esg.mekon.MekonConfig
import gsa.esg.mekon.run.{TestConfig, Configurator}
import gsa.esg.mekon.business.Region
import java.util.logging.Level
import gsa.esg.mekon.run.TestConfig.Feeds
import gsa.esg.mekon.components.adapters.{FeedSymbolMetaData, FeedDictionary, FeedRepository, FeedCollection}
import gsa.esg.mekon.feedtransforms.FeedObjectRef
import gsa.esg.mekon.components.adapters.FeedRepository.FeedConfig
import gsa.esg.mekon.components.trading.Feed
import scala.collection.mutable

class Scesspet {
    val leaves = collection.mutable.Set[AbsTerm[_,_]]()
    val allNodes = collection.mutable.Set[AbsTerm[_,_]]()

    def query[X](data: TraversableOnce[X], timeFunc:(X)=>Long) : Term[X] = {
      var root = new TraversableRoot[X](data, timeFunc)
      addNode(null, root)
      root
    }

    def query[X](newHasVal :(types.Env) => HasVal[X]) : Term[X] = {
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

  abstract class AbsTerm[IN, X](val source:AbsTerm[_, IN]) extends Term[X] {
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

    def map[Y](f: (X) => Y): Term[Y] = new MapTerm[X, Y](this, f)

    def filter(accept: (X) => Boolean): Term[X] = new FilterTerm[X](this, accept)
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

  class TraversableRoot[X](val data: TraversableOnce[X], timeFunc:(X)=>Long) extends RootTerm[X] {
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
}
