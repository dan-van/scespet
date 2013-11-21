package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import java.util.logging.Logger
import scespet.core.VectorStream.ReshapeSignal
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import scespet.core.{VectorStream, UpdatingHasVal}
import scespet.core.types.MFunc

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:10
 * To change this template use File | Settings | File Templates.
 */
class VectTerm[K,X](val env:types.Env)(val input:VectorStream[K,X]) extends MultiTerm[K,X] {
  import scala.collection.JavaConverters._
  import scala.collection.JavaConversions._

  def keys = input.getKeys.toList
  def values = input.getValues.toList

  /**
   * todo: call this "filterKey" ?
   */
  def subset(predicate:K=>Boolean):VectTerm[K,X] = mapKeys(k => {if (predicate(k)) Some(k) else None})

  /**
   * derives a new VectTerm with new derived keys (possibly a subset).
   * any mapping from K => Option[K2] that yields None will result in that K being dropped from the resulting vector
   * @param keyMap
   * @tparam K2
   * @return
   */
  def mapKeys[K2](keyMap:K=>Option[K2]):VectTerm[K2,X] = {
    new VectTerm[K2, X](env)(new ReKeyedVector[K, X, K2](input, keyMap, env))
  }

  def apply(k:K):MacroTerm[X] = {
    val index: Int = input.getKeys.indexOf(k)
    val cell:HasVal[X] = if (index >= 0) {
      var holder: HasValue[X] = input.getValueHolder(index)
      new HasVal[X] {
        def value = holder.value
        def trigger = holder.getTrigger
        def initialised = holder.initialised
      }
    } else {
      // construct an empty slot and bind it when the key appears
      val valueHolder = new ChainedCell[X]
      var newColumns: ReshapeSignal = input.getNewColumnTrigger
      env.addListener(newColumns, new types.MFunc {
        var searchedUpTo = input.getSize
        def calculate():Boolean = {
          for (i <- searchedUpTo to input.getSize - 1) {
            if (input.getKey(i) == k) {
              val inputCell = input.getValueHolder(i)
              valueHolder.bindTo(inputCell)
              if (inputCell.initialised()) {
                valueHolder.calculate() // this is possibly overkill - I'll write the tests then remove this line, but I like the idea of it being ready as soon as we respond to the new column
                env.wakeupThisCycle(valueHolder)
              }
              env.removeListener(newColumns, this)
              searchedUpTo = i
              return true
            }
          }
          searchedUpTo = input.getSize
          false
        }
      })
      valueHolder
    }
    new MacroTerm[X](env)(cell)
  }

  /** This doesn't work yet - questions of event atomicity / multiplexing */
  def by[K2]( keyMap:K=>K2 ):VectTerm[K2,X] = ???

  /** Experiment concept. To get round event atomicity, what about a Map[K2, Vect[K,X]] which is a partition of this vect?*/
  def groupby[K2]( keyMap:K=>K2 ):VectTerm[K2,VectorStream[K,X]] = {
    new VectTerm(env)(new NestedVector[K2, K, X](input, keyMap, env))
  }

  private class ChainedCell[C]() extends UpdatingHasVal[C] {
    var source: HasValue[C] = _
    var value = null.asInstanceOf[C]

    def calculate() = {
      value = source.value
      initialised = true
      true
    }

    def bindTo(newSource: HasValue[C]) {
      this.source = newSource
      env.addListener(newSource.getTrigger, this)
    }
  }

  class VectorMap[Y](f:VectorStream[K,X] => Y) extends UpdatingHasVal[Y] {
    // not convinced we should initialise like this?
    var value:Y = if (input.getSize > 0) {
      initialised = true
      f.apply(input)
    } else {
      println("Initial vector is empty, i.e. uninitialised. Is it right to apply the mapping function to the empty vector?")
      initialised = true
      f.apply(input)
    }

    def calculate() = {
      value = f.apply(input)
      true
    }
  }
  
  /**
   * This allows operations that operate on the entire vector rather than single cells (e.g. a demean operation, or a "unique value count")
   * I want to think more about other facilities on this line
   *
   * @return
   */
  def mapVector[Y](f:VectorStream[K,X] => Y):MacroTerm[Y] = {
    // build a vector where all cells share this single "collapsed" function
    val singleSharedCell = new VectorMap[Y](f)
    val cellBuilder = (index:Int, key:K) => singleSharedCell
    newIsomorphicVector(cellBuilder)
    // now return a single stream of the collapsed value:
    return new MacroTerm[Y](env)(singleSharedCell)
  }

  def map[Y: TypeTag](f:X=>Y, exposeNull:Boolean = true):VectTerm[K,Y] = {
    if ( (typeOf[Y] =:= typeOf[EventGraphObject]) ) println(s"WARNING: if you meant to listen to events from ${typeOf[Y]}, you should use 'derive'")
    class MapCell(index:Int) extends UpdatingHasVal[Y] {

      var value:Y = _
      def calculate() = {
        val inputValue = input.get(index)
        if (inputValue == null) {
          var isInitialised = input.getValueHolder(index).initialised()
          println(s"null input, isInitialised=$isInitialised")
        }
        val y = f(inputValue)
        if (exposeNull || y != null) {
          value = y
          initialised = true
          true
        } else {
          false
        }
      }
    }
    val cellBuilder = (index:Int, key:K) => new MapCell(index)
//    val cellBuilder = (index:Int, key:K) => {
//      val cellUpdateFunc = () => { f(input.get(index)) }
//      val initial = cellUpdateFunc()
//      new Generator[Y](initial, cellUpdateFunc)
//    }
    return newIsomorphicVector(cellBuilder)
  }

  // TODO: common requirement to specify a f(X) => Option[Y] as a filter.map chain
  // This compiles:
  // class Test[X](val in:X) {
  //  def myF[Y](f:X => Option[Y]):Test[Y] = {val oy = f(in); new Test(oy.getOrElse(null.asInstanceOf[Y]))}
  // }
  // new Test[Any]("Hello").myF(_ match {case x:Integer => Some(x);case _ => None}).in

  def filterType[Y : ClassTag]():VectTerm[K,Y] = {
    class MapCell(index:Int) extends UpdatingHasVal[Y] {
      //      var value = f(input.get(index)) // NOT NEEDED, as we generate a cell in response to an event, we auto-call calculate on init
      val classTag = reflect.classTag[Y]
      var value:Y = _
      def calculate() = {
        val inputVal = input.get(index)
        val oy = classTag.unapply(inputVal)
        if (oy.isDefined) {
          value = oy.get
          initialised = true
          true
        } else {
          false
        }
      }
    }
    val cellBuilder = (index:Int, key:K) => new MapCell(index)
    return newIsomorphicVector(cellBuilder)
  }

  def filter(accept: (X) => Boolean):VectTerm[K,X] = {
    class MapCell(index:Int) extends UpdatingHasVal[X] {
      //      var value = f(input.get(index)) // NOT NEEDED, as we generate a cell in response to an event, we auto-call calculate on init
      var value:X = _
      def calculate() = {
        val inputVal = input.get(index)
        if (accept(inputVal)) {
          value = inputVal
          initialised = true
          true
        } else {
          false
        }
      }
    }
    val cellBuilder = (index:Int, key:K) => new MapCell(index)
    return newIsomorphicVector(cellBuilder)
  }

  def fold_all[Y <: Reduce[X]](reduceBuilder : K => Y):VectTerm[K,Y] = {
    val cellBuilder = (index:Int, key:K) => new UpdatingHasVal[Y] {
      val value = reduceBuilder(key)
      // NOTE: The semantics of this field should be consistent with SlicedReduce.initialised
      initialised = false
      def calculate():Boolean = {
        val x: X = input.get(index)
        value.add(x)
        initialised = true
        return true
      }
    }
    return newIsomorphicVector(cellBuilder)
  }

  // todo: isn't this identical to the single Term reduceAll implementation?
  class ReduceAllCell[Y <: Reduce[X]](index:Int, key:K, reduceBuilder : K => Y) extends UpdatingHasVal[Y] {
    val termination = env.getTerminationEvent
    env.addListener(termination, this)

    var value:Y = null.asInstanceOf[Y]
    val reduction = reduceBuilder(key)

    def calculate():Boolean = {
      if (env.hasChanged(termination)) {
        value = reduction
        initialised = true
        true
      } else {
        val x: X = input.get(index)
        reduction.add(x)
        false
      }
    }
  }


  def reduce_all[Y <: Reduce[X]](reduceBuilder : K => Y):VectTerm[K,Y] = {
    val cellBuilder = (index:Int, key:K) => new ReduceAllCell[Y](index, key, reduceBuilder)
    return newIsomorphicVector(cellBuilder)
  }

//  def reduce_all[Y <: Reduce[X]](newBFunc:  => Y):VectTerm[K, Y] = {
//    // todo: why isn't this the same shape as fold_all?
//    val newBucketAsFunc = () => newBFunc
//    val chainedVector = new ChainedVector[K, SlicedReduce[X, Y], Y](input, env) {
//      def newCell(i: Int, key: K): SlicedReduce[X, Y] = {
//        val sourceHasVal = input.getValueHolder(i)
//        new SlicedReduce[X, Y](sourceHasVal, null, false, newBucketAsFunc, ReduceType.LAST, env)
//      }
//    }
//    return new VectTerm[K,Y](env)(chainedVector)
//  }


  private def newIsomorphicVector[Y](cellBuilder: (Int, K) => UpdatingHasVal[Y]): VectTerm[K, Y] = {
    val output: VectorStream[K, Y] = new ChainedVector[K, Y](input, env) {
      def newCell(i: Int, key: K): UpdatingHasVal[Y] = {
        val cellFunc: UpdatingHasVal[Y] = cellBuilder.apply(i, key)
        val sourceCell = input.getValueHolder(i)
        val sourceTrigger: EventGraphObject = sourceCell.getTrigger()
        env.addListener(sourceTrigger, cellFunc)
        // initialise the cell
        val hasInputValue = sourceCell.initialised()
        val hasChanged = env.hasChanged(sourceTrigger)
        if (hasChanged && !hasInputValue) {
          println("WARN: didn't expect this")
        }
        val oldIsInitialised = hasInputValue || hasChanged
        if (oldIsInitialised) {
          val hasInitialOutput = cellFunc.calculate()

          if (hasInitialOutput && ! cellFunc.initialised) {
            throw new AssertionError("Cell should have been initialised by calculate: "+cellFunc+" for key "+key)
          }
        }
        return cellFunc
      }
    }
    return new VectTerm[K, Y](env)(output)
  }

  /**
   * used to build a set from the values in a vector
   * the new vector acts like a set (key == value), generated values are folded into it.
   *
   * todo: maybe call this "flatten", "asSet" ?
   */
  def toValueSet[Y]( expand: (X=>TraversableOnce[Y]) = ( (x:X) => Traversable(x.asInstanceOf[Y]) ) ):VectTerm[Y,Y] = {
    val initial = collection.mutable.Set[Y]()
    for (x <- input.getValues.asScala; y <- expand(x)) {
      initial += y
    }
    val flattenedSet = new MutableVector[Y](initial.toIterable.asJava, env) with types.MFunc {
      val newColumnsTrigger = input.getNewColumnTrigger
      env.addListener(newColumnsTrigger, this)
      var maxTriggerIdx = 0

      private def bindNewCells() {
        for (i <- maxTriggerIdx to input.getSize - 1) {
          var x = input.get(i)
          // expand x and add elements
          this.addAll( expand(x).toIterable.asJava)
          // install a listener to keep doing this
          var cellTrigger = input.getTrigger(i)
          env.addListener(cellTrigger, new types.MFunc() {
            def calculate(): Boolean = {
              var x = input.get(i)
              val added = addAll( expand(x).toIterable.asJava)
              added
            }
          })
        }
        maxTriggerIdx = input.getSize
      }

      def calculate(): Boolean = {
        if (env.hasChanged(newColumnsTrigger)) {
          bindNewCells()
        }
        true
      }
    }
    return new VectTerm[Y, Y](env)(flattenedSet)
  }

  /**
   * derive a new vector by applying a function to the keys of the current vector.
   * The new vector will have the same keys, but different values.
   *
   * TODO: naming
   * this is related to "map", but "map" is a function of value, this is a function of key
   * maybe this should be called 'mapkey', or 'takef'? (i.e. we're 'taking' cells from the domain 'cellFromKey'?)
   * the reason I chose 'join' is that we're effectively doing a left join of this vector onto a vector[domain, cellFromKey]
   @param cellFromKey
   * @tparam Y
   * @return
   */
  def derive[Y]( cellFromKey:K=>HasVal[Y] ):VectTerm[K,Y] = {
    val output: VectorStream[K, Y] = new ChainedVector[K, Y](input, env) {
      def newCell(i: Int, key: K) = cellFromKey(key)
    }
    return new VectTerm[K, Y](env)(output)
  }


  /**
   * todo: naming
   * build a vector of sliced Bucket instances with the same shape as this vector
   *
   * @param newBFunc
   * @tparam B
   * @return
   */
  def deriveSliced[B <: Bucket[_]](newBFunc: K => B):PreThing[K, B] = new PreThing[K, B](newBFunc, VectTerm.this.input, env)

  
  def join[Y, K2]( other:VectTerm[K2,Y], keyMap:K => K2) :VectTerm[K,(X,Y)] = {
    return new VectTerm(env)(new VectorJoin[K, K2, X, Y](input, other.input, env, keyMap, fireOnOther=true))
  }

  def take[Y, K2]( other:VectTerm[K2,Y], keyMap:K => K2) :VectTerm[K,(X,Y)] = {
    return new VectTerm(env)(new VectorJoin[K, K2, X, Y](input, other.input, env, keyMap, fireOnOther=false))
  }


  /**
   * we could build this out of other primitives (e.g. reduce, or 'take(derive).map') but this is more convenient and efficient
   * @param evt
   * @return
   */
  def sample(evt:EventGraphObject):VectTerm[K,X] = {
    val output: VectorStream[K, X] = new ChainedVector[K, X](input, env) {
      def newCell(i: Int, key: K) = new UpdatingHasVal[X] {
        var value:X = _
        env.addListener(evt, this)

        def calculate() = {
          value = input.get(i)
          initialised = true
          true
        }
      }
    }
    return new VectTerm[K, X](env)(output)
  }

  /**
   * build a vector of Reduce instances that are driven with values from this vector.
   * the Reduce instances do not expose intermediate state, but will only fire when the bucket is closed.
   * e.g. if 'Sum' then we are exposing the total summation once the bucket is sealed (e.g. daily trade volume)
   * @see #fold
   * @param newBFunc
   * @tparam Y
   * @return
   */
  def reduce[Y <: Reduce[X]](newBFunc: K => Y):BucketBuilderVect[K, Y] = new BucketBuilderVectImpl[K, X,Y](newBFunc, VectTerm.this, ReduceType.LAST, env)

  /**
   * build a vector of Reduce instances that are driven with values from this vector.
   * the Reduce instances expose their state changes.
   * e.g. if 'Sum' then we are exposing the current cumulative sum as time goes by (e.g. accumulated trade volume)
   * @see #reduce
   * @param newBFunc
   * @tparam Y
   * @return
   */
  def fold[Y <: Reduce[X]](newBFunc: K => Y):BucketBuilderVect[K, Y] = new BucketBuilderVectImpl[K, X,Y](newBFunc, VectTerm.this, ReduceType.CUMULATIVE, env)
}

class PreThing[K,B <: Bucket[_]](newBFunc: K => B, input:VectorStream[K, _], env:types.Env) {
  def fold() = new Thing(newBFunc, input, ReduceType.CUMULATIVE, env)
  def reduce() = new Thing(newBFunc, input, ReduceType.LAST, env)
}

class Thing[K, B <: Bucket[_]](newBFunc: K => B, input:VectorStream[K, _], emitType:ReduceType, env:types.Env) {
  private var joins = List[BucketJoin[K, _, B]]()

  def join[X](term:VectTerm[K, X])(adder :B=>X=>Unit) :Thing[K, B] = {
    joins :+= new BucketJoin[K, X, B](term.input, adder)
    this
  }

  def slice_post(trigger: EventGraphObject):VectTerm[K,B] = {
    val sliceTrigger = trigger
    val bucketJoinVector = new MultiVectorJoin[K, B](input, sliceTrigger, newBFunc, joins, emitType, env)
    return new VectTerm[K,B](env)(bucketJoinVector)
  }
}