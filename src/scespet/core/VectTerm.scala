package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import java.util.logging.Logger
import scespet.core.VectorStream.ReshapeSignal
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag
import scespet.core.UpdatingHasVal

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:10
 * To change this template use File | Settings | File Templates.
 */
class VectTerm[K,X](val env:types.Env)(val input:VectorStream[K,X]) extends MultiTerm[K,X] {
  import scala.reflect.macros.Context
  import scala.collection.JavaConverters._
  import scala.collection.JavaConversions._

  def keys = input.getKeys.toList
  def values = input.getValues.toList

  /**
   * todo: call this "filterKey" ?
   */
  def subset(predicate:K=>Boolean):VectTerm[K,X] = {
    val output: VectorStream[K, X] = new ChainedVector[K, UpdatingHasVal[X], X](input, env) {
      val sourceIndicies = collection.mutable.ArrayBuffer[Integer]()

      override def add(key: K) {
        if (predicate.apply(key)) {
          val sourceIndex = input.getKeys.indexOf(key)
          if (getIndex(key) == -1) {
            sourceIndicies += sourceIndex
            super.add(key)
          }
        }
      }

      def newCell(newIndex: Int, key: K): UpdatingHasVal[X] = {
        val sourceCell = input.getValueHolder(newIndex)
        val sourceTrigger: EventGraphObject = sourceCell.getTrigger()
        class MapCell(index:Int) extends UpdatingHasVal[X] {
          //      var value = f(input.get(index)) // NOT NEEDED, as we generate a cell in response to an event, we auto-call calculate on init
          var value:X = _
          def calculate() = {
            value = sourceCell.value()
            true
          }
        }
        val cellFunc = new MapCell(newIndex)
        env.addListener(sourceTrigger, cellFunc)
        // initialise the cell
        val sourceIndex = sourceIndicies(newIndex)
        val hasInputValue = input.getNewColumnTrigger.newColumnHasValue(sourceIndex)
        val hasChanged = env.hasChanged(sourceTrigger)
        if (hasChanged && !hasInputValue) {
          println("WARN: didn't expect this")
        }
        if (hasInputValue || hasChanged) {
          val hasInitialOutput = cellFunc.calculate()
          getNewColumnTrigger.newColumnAdded(newIndex, hasInitialOutput)
        }
        return cellFunc
      }
    }
    return new VectTerm[K, X](env)(output)
  }

  def apply(k:K):MacroTerm[X] = {
    val index: Int = input.getKeys.indexOf(k)
    val cell:HasVal[X] = if (index >= 0) {
      var holder: HasValue[X] = input.getValueHolder(index)
      new HasVal[X] {
        def value = holder.value
        def trigger = holder.getTrigger
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
              if (newColumns.newColumnHasValue(i)) {
                valueHolder.calculate()
              }
              valueHolder.bindTo(input.getValueHolder(i))
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

  def by[K2]( keyMap:K=>K2 ):VectTerm[K2,X] = {
    // todo: I've not finished the impl of ReKeyedVector
    new VectTerm[K2, X](env)(new ReKeyedVector[K, X, K2](input, keyMap, env))
  }

  private class ChainedCell[C]() extends UpdatingHasVal[C] {
    var source: HasValue[C] = _
    var value = null.asInstanceOf[C]

    def calculate() = {
      value = source.value
      true
    }

    def bindTo(newSource: HasValue[C]) {
      this.source = newSource
      env.addListener(newSource.getTrigger, this)
    }
  }

  /**
   * This allows operations that operate on the entire vector rather than single cells (e.g. a demean operation, or a "unique value count")
   * I want to think more about other facilities on this line
   *
   * @return
   */
  def mapVector[Y](f:VectorStream[K,X] => Y):MacroTerm[Y] = {
    val collapsed = new UpdatingHasVal[Y] {

      val value:Y = f.apply(input)

      def calculate() = {
        f.apply(input)
        true
      }
    }
    // build a vector where all cells share this single "collapsed" function
    val cellBuilder = (index:Int, key:K) => collapsed
    newIsomorphicVector(cellBuilder)
    // now return a single stream of the collapsed value:
    return new MacroTerm[Y](env)(collapsed)
  }

  def map[Y: TypeTag](f:X=>Y):VectTerm[K,Y] = {
    if ( (typeOf[Y] =:= typeOf[EventGraphObject]) ) println(s"WARNING: if you meant to listen to events from ${typeOf[Y]}, you should use 'derive'")
    class MapCell(index:Int) extends UpdatingHasVal[Y] {
//      var value = f(input.get(index)) // NOT NEEDED, as we generate a cell in response to an event, we auto-call calculate on init
      var value:Y = _
      def calculate() = {
        var in = input.get(index)
        if (in == null) {
          println("null input")
        }
        value = f(in);
        true
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
      def calculate():Boolean = {
        val x: X = input.get(index)
        value.add(x)
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
    val output: VectorStream[K, Y] = new ChainedVector[K, UpdatingHasVal[Y], Y](input, env) {
      def newCell(i: Int, key: K): UpdatingHasVal[Y] = {
        val cellFunc: UpdatingHasVal[Y] = cellBuilder.apply(i, key)
        var sourceCell = input.getValueHolder(i)
        val sourceTrigger: EventGraphObject = sourceCell.getTrigger()
        env.addListener(sourceTrigger, cellFunc)
        // initialise the cell
        var hasInputValue = input.getNewColumnTrigger.newColumnHasValue(i)
        var hasChanged = env.hasChanged(sourceTrigger)
        if (hasChanged && !hasInputValue) {
          println("WARN: didn't expect this")
        }
        if (hasInputValue || hasChanged) {
          val hasInitialOutput = cellFunc.calculate()
          getNewColumnTrigger.newColumnAdded(i, hasInitialOutput)
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
    val output: VectorStream[K, Y] = new ChainedVector[K, EventGraphObject, Y](input, env) {
      def newCell(i: Int, key: K) = cellFromKey(key)
    }
    return new VectTerm[K, Y](env)(output)
  }

  /**
   * derive a new vector with the same key, but elements generated from the current element's key and listenable value holder
   * e.g. we could have a vector of name->RandomStream and generate a derived
   * todo: should we delete the derive method and always pass (k,v)
   * @param cellFromEntry
   * @tparam Y
   * @return
   */
  def derive2[Y]( cellFromEntry:(K,X)=>HasVal[Y] ):VectTerm[K,Y] = {
    val output: VectorStream[K, Y] = new ChainedVector[K, EventGraphObject, Y](input, env) {
      def newCell(i: Int, key: K) = {
        val valuePresent = input.getNewColumnTrigger.newColumnHasValue(i)
        if (!valuePresent) {
          // not sure what to do
          cellFromEntry(key, input.get(i))
        } else {
          cellFromEntry(key, input.get(i))
        }
      }
    }
    return new VectTerm[K, Y](env)(output)
  }

  def join[Y]( other:VectTerm[K,Y] ):VectTerm[K,(X,Y)] = {
    return new VectTerm(env)(new VectorJoin[K, X, Y](input, other.input, env))
  }

  def sample(evt:EventGraphObject):VectTerm[K,X] = {
    val output: VectorStream[K, X] = new ChainedVector[K, EventGraphObject, X](input, env) {
      def newCell(i: Int, key: K) = new UpdatingHasVal[X] {
        var value:X = _
        env.addListener(evt, this)

        def calculate() = {
          value = input.get(i)
          true
        }
      }
    }
    return new VectTerm[K, X](env)(output)
  }

  def reduce[Y <: Reduce[X]](newBFunc: K => Y):BucketBuilderVect[K, X, Y] = new BucketBuilderVectImpl[K, X,Y](newBFunc, VectTerm.this, ReduceType.LAST, env)

  def fold[Y <: Reduce[X]](newBFunc: K => Y):BucketBuilderVect[K, X, Y] = new BucketBuilderVectImpl[K, X,Y](newBFunc, VectTerm.this, ReduceType.CUMULATIVE, env)
}
