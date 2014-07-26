package scespet.core

import gsa.esg.mekon.core.{Environment, EventGraphObject}
import scespet.core.VectorStream.ReshapeSignal
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag


import scespet.util._
import scala.Some

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
  def entries = keys.zip(values)

  /**
   * todo: call this "filterKey" ?
   */
  def subset(predicate:K=>Boolean):VectTerm[K,X] = mapKeys(k => {if (predicate(k)) Some(k) else None})

  /**
   * when this is MultiTerm[X,X] (i.e. a set), then you can present it as a Stream[List[X]]
   * @return
   */
  def keyList()(implicit ev:K =:= X):MacroTerm[List[K]] = {
    val keySetHolder : HasVal[List[K]] = new HasVal[List[K]] {
      def value = input.getKeys.toList

      def trigger = input.getNewColumnTrigger

      def initialised = true
    }
    new MacroTerm[List[K]](env)(keySetHolder)
  }

  /**
   * derives a new VectTerm with new derived keys (possibly a subset).
   * any mapping from K => Option[K2] that yields None will result in that K being dropped from the resulting vector
   * todo: more thought - this may be more useful to use null instread of 'None' as it avoids having to introduce Option
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
                env.wakeupThisCycle(valueHolder)  // damn - this is firing before fireAfterChangingListeners catches the underlying cell
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
    var value:Y = if (input.isInitialised) {
      initialised = true
      f.apply(input)
    } else {
      println("Initial vector is uninitialised. not applying mapVector to the empty vector")
      initialised = false
      null.asInstanceOf[Y]
    }

    def calculate() = {
      value = f.apply(input)
      initialised = true
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

  def fold_all[Y <: Agg[X]](reduceBuilder : K => Y):VectTerm[K,Y#OUT] = {
    val cellBuilder = (index:Int, key:K) => new UpdatingHasVal[Y#OUT] {
      val agg:Y = reduceBuilder(key)
      def value = agg.asInstanceOf[Y].value
      // NOTE: The semantics of this field should be consistent with SlicedReduce.initialised
      initialised = false
      def calculate():Boolean = {
        val x: X = input.get(index)
        agg.add(x)
        initialised = true
        return true
      }
    }
    return newIsomorphicVector(cellBuilder)
  }

//  def reduce_all[Y <: Agg[X]](reduceBuilder : K => Y):VectTerm[K,Y#OUT] = {
//    val cellBuilder = (index:Int, key:K) => new ReduceAllCell[K, X, Y](env, input, index, key, reduceBuilder, ReduceType.LAST)
//    return newIsomorphicVector(cellBuilder)
//  }
//
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


  private [core] def newIsomorphicVector[Y](cellBuilder: (Int, K) => UpdatingHasVal[Y]): VectTerm[K, Y] = {
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
        if (oldIsInitialised && !cellFunc.initialised) {
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

  def toKeySet() = {
    new VectTerm[K,K](env)(new ChainedVector[K, K](input, env) {
      def newCell(i: Int, key: K) = {
        val cell = new ValueFunc[K](env)
        cell.setValue(key)
        cell
      }
    })
  }

  /**
   * This is like "map", but the outputs of the map function are flattened and presented as a MultiStream (acting as a set, i.e. key == value).
   *
   * An example usage is when we have a stream that is exposing a batch of new elements on each fire (e.g. a Dictionary that notifies of batches of new elements)
   * We can transform this stream (or multistream) into a dynamically growing set of the unique values
   *
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
      // now bind any existing cells
      bindNewCells()

      private def bindNewCells() {
        for (i <- maxTriggerIdx to input.getSize - 1) {
          val x = input.get(i)
          // expand x and add elements
          this.addAll( expand(x).toIterable.asJava)
          // install a listener to keep doing this
          val cellTrigger = input.getTrigger(i)
          env.addListener(cellTrigger, new types.MFunc() {
            def calculate(): Boolean = {
              val x = input.get(i)
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
  def keyToStream[Y]( cellFromKey:K=>HasVal[Y] ):VectTerm[K,Y] = {
    val output: VectorStream[K, Y] = new ChainedVector[K, Y](input, env) {
      def newCell(i: Int, key: K) = cellFromKey(key)
    }
    return new VectTerm[K, Y](env)(output)
  }

//  def keyToStream2[Y, SY : ToHasVal[SY, Y]]( cellFromKey:K=>SY ):VectTerm[K,Y] = {
//    val keyToHasVal = cellFromKey.andThen( implicitly[ToHasVal[SY, Y]].toHasVal )
//    keyToStream(keyToHasVal)
//  }

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
   * the Reduce instances expose their state changes.
   * e.g. if 'Sum' then we are exposing the current cumulative sum as time goes by (e.g. accumulated trade volume)
   * @see #reduce
   * @param newBFunc
   * @tparam Y
   * @return
   */
  def fold[Y <: Agg[X]](newBFunc: K => Y):BucketBuilderVect[K, Y#OUT] = ???
  //new BucketBuilderVectImpl[K, X,Y](newBFunc, VectTerm.this, ReduceType.CUMULATIVE, env)

  // THINK: this could be special cased to be faster
//  def reduce[Y <: Agg[X]](newBFunc: K => Y):VectTerm[K, Y#OUT] = group[Any](null, AFTER)(SliceTriggerSpec.TERMINATION).reduce(newBFunc)

  def reduce[Y <: Agg[X]](newBFunc: => Y):VectTerm[K, Y#OUT] = reduce[Y]((k:K) => newBFunc)
  // THINK: this could be special cased to be faster
  def reduce[Y <: Agg[X]](newBFunc: K => Y):VectTerm[K, Y#OUT] = group[Any](null, AFTER)(SliceTriggerSpec.TERMINATION.asVectSliceSpec).reduce(newBFunc)

  def scan[Y <: Agg[X]](newBFunc: => Y):VectTerm[K, Y#OUT] = scan[Y]((k:K) => newBFunc)
  // THINK: this could be special cased to be faster
  def scan[Y <: Agg[X]](newBFunc: K => Y) = group[Null](null, AFTER)(SliceTriggerSpec.NULL.asVectSliceSpec).scan(newBFunc)

  def bindTo[B <: Bucket](newBFunc: => B)(adder: B => X => Unit) :PartialBuiltSlicedVectBucket[K, B] = bindTo[B]((k:K) => newBFunc)(adder)

  def bindTo[B <: Bucket](newBFunc: K => B)(adder: B => X => Unit) :PartialBuiltSlicedVectBucket[K, B] = {
    val keyToCellLifecycle = new KeyToSliceCellLifecycle[K, B] {
      override def lifeCycleForKey(k: K): SliceCellLifecycle[B] = new BucketCellLifecycleImpl[B](newBFunc(k))
    }
    new PartialBuiltSlicedVectBucket[K, B](this, keyToCellLifecycle, env).bind(this)(adder)
  }

  def group[S](sliceSpec:S, triggerAlign:SliceAlign = AFTER)(implicit ev:VectSliceTriggerSpec[S]) :GroupedVectTerm[K, X] = {
    val uncollapsed = new UncollapsedVectGroupWithTrigger[S, K, X](input, sliceSpec, triggerAlign, env, ev)
    val grouped = new GroupedVectTerm[K, X](this, uncollapsed, env)
//NODEPLOY
//    grouped.newCellsDependOn(windowVect)
    grouped
  }

  // THINK: may want to go with TypeClasses here?
  def window(windowStream:HasVal[Boolean]):GroupedVectTerm[K, X] = window(_ => windowStream)
  def window(windowVect:VectTerm[K, Boolean]):GroupedVectTerm[K, X] = {
    val grouped = window((k:K) => windowVect(k))
// TODO: we may need to define ordering for pre-requisites of new cells?
    grouped.newCellsDependOn(windowVect)
    grouped
  }
  def window(keyToWindow:K => HasVal[Boolean]):GroupedVectTerm[K, X] = {
    val uncollapsed = new UncollapsedVectGroup[K, X] {
      override def applyB[B <: Bucket](keyedLifecycle: KeyToSliceCellLifecycle[K, B], reduceType: ReduceType): (Int, K) => UpdatingHasVal[B#OUT] = {
        (i:Int, k:K) => {
          val sourceCell = input.getValueHolder(i)
          val lifecycle = keyedLifecycle.lifeCycleForKey(k)
          val windowStream = keyToWindow(k)
          val outputCell:UpdatingHasVal[B#OUT] = reduceType match {
            case ReduceType.CUMULATIVE => new WindowedBucket_Continuous[B](windowStream, lifecycle, env)
            case ReduceType.LAST => new WindowedBucket_LastValue[B](windowStream, lifecycle, env)
          }
          outputCell
        }
      }

      override def applyAgg[A <: Agg[X]](keyedLifecycle: KeyToSliceCellLifecycle[K, A], reduceType: ReduceType): (Int, K) => UpdatingHasVal[A#OUT] = {
        (i:Int, k:K) => {
          val sourceCell = input.getValueHolder(i)
          val lifecycle = keyedLifecycle.lifeCycleForKey(k)
          val windowStream = keyToWindow(k)
          val outputCell:UpdatingHasVal[A#OUT] = new WindowedReduce[X, A](sourceCell, windowStream, lifecycle, reduceType, env)
          outputCell
        }
      }
    }
    new GroupedVectTerm[K, X](this, uncollapsed, env)
  }
}

//class PreSliceBuilder[K,B <: Bucket](newBFunc: K => B, input:VectorStream[K, _], env:types.Env) {
//  def fold() = new SliceBuilder(newBFunc, input, ReduceType.CUMULATIVE, env)
//  def reduce() = new SliceBuilder(newBFunc, input, ReduceType.LAST, env)
//}

class SliceBuilder[K, OUT, B <: Bucket](cellLifecycle:SliceCellLifecycle[B], input:VectorStream[K, _], emitType:ReduceType, env:types.Env) {
  private var joins = List[BucketJoin[K, _, B]]()

  def join[X](term:VectTerm[K, X])(adder :B=>X=>Unit) :SliceBuilder[K, OUT, B] = {
    joins :+= new BucketJoin[K, X, B](term.input, adder)
    this
  }

  def each(n:Int):VectTerm[K,B] = {
//    val bucketJoinVector = new MultiVectorJoin[K, OUT, B](input, joins, env) {
//      def createBucketCell(key:K): SlicedBucket[OUT, B] = {
//        val newBFuncFromKey = () => newBFunc(key)
//        val bucketStreamCell = new SliceAfterBucket[OUT, B](slice, cellLifecycle, emitType, env)
////        return bucketStreamCell
//        ???
//      }
//    }
//    return new VectTerm[K,B](env)(bucketJoinVector)
    ???
  }

  /**
   * window the whole vector by a single bucket stream (e.g. 9:00-17:00 EU)
   * A window-open happens-before a new value to be added (i.e. the new value is included in the window)
   * A window-close happens-before a new value (i.e if the window close event is atomic with a value for the bucket, that value is deemed to be not-in the bucket)

   * @param windowStream
   * @return
   */
  def window(windowStream: MacroTerm[Boolean]) :VectTerm[K, B] = {
    // all cells use the same single window trigger
    window(cellkey => windowStream.input)
  }

  /**
   * window each element in the vector with the given window function
   * @return
   */
  def window(windowFunc: K => HasValue[Boolean]) :VectTerm[K, B] = {
//    val bucketJoinVector = new MultiVectorJoin[K, B](input, joins, env) {
//      def createBucketCell(key:K): BucketCell[B] = {
//        val newBFuncFromKey = () => newBFunc(key)
//        val cellWindowFunc = windowFunc(key)
//        emitType match {
//          case ReduceType.LAST => new WindowedBucket_LastValue[B](cellWindowFunc, newBFuncFromKey, env)
//          case ReduceType.CUMULATIVE => new WindowedBucket_Continuous[B](cellWindowFunc, newBFuncFromKey, env)
//        }
//      }
//    }
//    return new VectTerm[K,B](env)(bucketJoinVector)
    ???
  }

  /**
   * do a takef on the given vector to get hasValue[Boolean] for each key in this vector.
   * if the other vector does not have the given key, the window will be assumed to be false (i.e. not open)
   * @return
   */
  def window(windowVect: VectTerm[K,Boolean]) :VectTerm[K, B] = {
//    val bucketJoinVector = new MultiVectorJoin[K, B](input, joins, env) {
//      def createBucketCell(key:K): BucketCell[B] = {
//        val newBFuncFromKey = () => newBFunc(key)
//        val cellWindowFunc = windowVect(key)
//        emitType match {
//          case ReduceType.LAST => new WindowedBucket_LastValue[B](cellWindowFunc.input, newBFuncFromKey, env)
//          case ReduceType.CUMULATIVE => new WindowedBucket_Continuous[B](cellWindowFunc.input, newBFuncFromKey, env)
//        }
//      }
//    }
//    return new VectTerm[K,B](env)(bucketJoinVector)
    ???
  }
}

class VectAggLifecycle[K, X, Y <: Agg[X]](newCellF: K => Y) extends KeyToSliceCellLifecycle[K,Y]{
  override def lifeCycleForKey(k: K): SliceCellLifecycle[Y] = new AggSliceCellLifecycle[X, Y](newCellF(k))
}

class GroupedVectTerm[K, X](val input:VectTerm[K,X], val uncollapsedGroup: UncollapsedVectGroup[K, X], val env:types.Env) {
  var orderDepends = Seq[VectTerm[K,_]]()
  private [core] def newCellsDependOn(term: VectTerm[K, _]) = {
    orderDepends :+= term
  }

  def reduce[Y <: Agg[X]](newBFunc:  => Y)(implicit ev:Y <:< Agg[X]) :VectTerm[K, Y#OUT] = reduce[Y]((k:K) => newBFunc)(ev)
  def reduce[Y <: Agg[X]](newBFunc: K => Y)(implicit ev:Y <:< Agg[X]) :VectTerm[K, Y#OUT] = {
    val lifecycle :VectAggLifecycle[K, X, Y] = new VectAggLifecycle[K, X, Y](newBFunc)
    val cellBuilder = uncollapsedGroup.applyAgg[Y](lifecycle, ReduceType.LAST)
    val newVect = input.newIsomorphicVector[Y#OUT](cellBuilder)
    for (dep <- orderDepends) {
      env.addListener(dep.input.getNewColumnTrigger, newVect.input.getNewColumnTrigger)
    }
    return newVect
  }

  def scan[Y <: Agg[X]](newBFunc: => Y)(implicit ev:Y <:< Agg[X]) :VectTerm[K, Y#OUT] = scan[Y]((k:K) => newBFunc)(ev)
  def scan[Y <: Agg[X]](newBFunc: K => Y)(implicit ev:Y <:< Agg[X]) :VectTerm[K, Y#OUT] = {
    val lifecycle :VectAggLifecycle[K, X, Y] = new VectAggLifecycle[K, X, Y](newBFunc)
    val cellBuilder = uncollapsedGroup.applyAgg[Y](lifecycle, ReduceType.CUMULATIVE)
    val newVect = input.newIsomorphicVector[Y#OUT](cellBuilder)
    for (dep <- orderDepends) {
      env.addListener(dep.input.getNewColumnTrigger, newVect.input.getNewColumnTrigger)
    }
    return newVect
  }
}

trait UncollapsedVectGroup[K, IN] {
  def applyB[B <: Bucket](lifecycle:KeyToSliceCellLifecycle[K, B], reduceType:ReduceType) :(Int,K)=>UpdatingHasVal[B#OUT]
  def applyAgg[A <: Agg[IN]](lifecycle:KeyToSliceCellLifecycle[K, A], reduceType:ReduceType) :(Int,K)=>UpdatingHasVal[A#OUT]
}

trait DerivedVector[K, OUT] {
  def newCell(i:Int, k:K):UpdatingHasVal[OUT]
  def dependsOn:Set[EventGraphObject]
}

class UncollapsedVectGroupWithTrigger[S, K, IN](inputVector:VectorStream[K, IN], sliceSpec: S, triggerAlign:SliceAlign, env:types.Env, ev: VectSliceTriggerSpec[S]) extends UncollapsedVectGroup[K, IN] {
  def applyAgg[A <: Agg[IN]](lifecycle: KeyToSliceCellLifecycle[K, A], reduceType:ReduceType): (Int, K) => UpdatingHasVal[A#OUT] = {
    (i:Int, k:K) => {
      val sourceCell = inputVector.getValueHolder(i)
      val cellLifecycle = lifecycle.lifeCycleForKey(k)
      val sliceSpecEv = ev.toTriggerSpec(k, sliceSpec)
      println("New sliced reduce for key: "+k+" from source: "+inputVector)
      new SlicedReduce[S, IN, A](sourceCell, sliceSpec, triggerAlign == BEFORE, cellLifecycle, reduceType, env, sliceSpecEv)
    }
  }

  def applyB[B <: Bucket](lifecycle: KeyToSliceCellLifecycle[K, B], reduceType: ReduceType): (Int, K) => UpdatingHasVal[B#OUT] = {
    triggerAlign match {
      case BEFORE => {
        (i:Int, k:K) => {
          val sourceCell = inputVector.getValueHolder(i)
          val cellLifecycle = lifecycle.lifeCycleForKey(k)
          val sliceSpecEv = ev.toTriggerSpec(k, sliceSpec)
          new SliceBeforeBucket[S, B](sliceSpec, cellLifecycle, reduceType, env, sliceSpecEv)
        }
      }
      case AFTER => {
        (i:Int, k:K) => {
          val sourceCell = inputVector.getValueHolder(i)
          val cellLifecycle = lifecycle.lifeCycleForKey(k)
          val sliceSpecEv = ev.toTriggerSpec(k, sliceSpec)
          new SliceAfterBucket[S, B](sliceSpec, cellLifecycle, reduceType, env, sliceSpecEv)
        }
      }
      case _ => throw new IllegalArgumentException(String.valueOf(triggerAlign))
    }
  }
}

// NODEPLOY this should be a Term that also supports partitioning operations
class PartialBuiltSlicedVectBucket[K, Y <: Bucket](input:VectTerm[K, _], val keyCellLifecycle: KeyToSliceCellLifecycle[K, Y], val env:Environment) {
  var bindings = List[(VectTerm[K, _], (_ => _ => Unit))]()

  private lazy val scanAllTerm: VectTerm[K, Y#OUT] = {
    reset[Null](null)(SliceTriggerSpec.NULL).all()
//    val cellBuilder = (i:Int, k:K) => {
//      val cellLifecycle = keyCellLifecycle.lifeCycleForKey(k)
//      val slicer = new SliceAfterBucket[Null, Y](null, cellLifecycle, ReduceType.CUMULATIVE, env, SliceTriggerSpec.NULL)
//      // add the captured bindings
//      bindings.foreach(pair => {
//        val (vectTerm, adder) = pair
//        type IN = Any
//        val hasVal = vectTerm(k).input
//        slicer.addInputBinding[IN](hasVal.asInstanceOf[HasVal[IN]], adder.asInstanceOf[Y => IN => Unit])
//      })
//      slicer
//    }
//    input.newIsomorphicVector(cellBuilder)
  }


  def last(): VectTerm[K, Y#OUT] = {
    reset[Null](null)(SliceTriggerSpec.NULL).last()
//    val cellBuilder = (i:Int, k:K) => {
//      val cellLifecycle = keyCellLifecycle.lifeCycleForKey(k)
//      val slicer = new SliceBeforeBucket[Any, Y](null, cellLifecycle, ReduceType.LAST, env, SliceTriggerSpec.TERMINATION)
//      // add the captured bindings
//      bindings.foreach(pair => {
//        val (hasVal, adder) = pair
//        type IN = Any
//        slicer.addInputBinding[IN](hasVal.asInstanceOf[HasVal[IN]], adder.asInstanceOf[Y => IN => Unit])
//      })
//      slicer
//    }
//    input.newIsomorphicVector(cellBuilder)
  }

  // NODEPLOY - delegate remaining Term interface calls here using lazyVal approach
  def all(): VectTerm[K, Y#OUT] = scanAllTerm

  def bind[S](stream: VectTerm[K, S])(adder: Y => S => Unit): PartialBuiltSlicedVectBucket[K, Y] = {
    bindings :+=(stream, adder)
    this
  }

  // NODEPLOY - I think this would be better named as 'reset', once you already have a stream->reducer binding, talking about grouping is confusing.
  //NODEPLOY - think:
  // CellLifecycle creates a new cell at beginning of stream, then multiple calls to close bucket after a slice
  // this avoids needing a new slice trigger definition each slice.
  def reset[S](sliceSpec: S, triggerAlign: SliceAlign = AFTER)(implicit ev: SliceTriggerSpec[S]):PartialGroupedVectBucketStream[K, S, Y] = {
    new PartialGroupedVectBucketStream[K, S, Y](input, triggerAlign, keyCellLifecycle, bindings, sliceSpec, ev, env)
  }
}

class PartialGroupedVectBucketStream[K, S, Y <: Bucket](input:VectTerm[K,_],
                                                        triggerAlign:SliceAlign,
                                                        keyCellLifecycle:KeyToSliceCellLifecycle[K, Y],
                                                        bindings :List[(VectTerm[K, _], (_ => _ => Unit))],
                                                        sliceSpec:S, ev:SliceTriggerSpec[S], env:types.Env) {
  private def buildSliced(reduceType:ReduceType) :VectTerm[K, Y#OUT] = {
    val cellBuilder = (i:Int, k:K) => {
      val cellLifecycle = keyCellLifecycle.lifeCycleForKey(k)
      val slicer = triggerAlign match {
        case BEFORE => new SliceBeforeBucket[S, Y](sliceSpec, cellLifecycle, reduceType, env, ev)
        case AFTER => new SliceAfterBucket[S, Y](sliceSpec, cellLifecycle, reduceType, env, ev)
      }
      // add the captured bindings
      bindings.foreach(pair => {
        val (vectTerm, adder) = pair
        type IN = Any
        val hasVal = vectTerm(k).input
        slicer.addInputBinding[IN](hasVal.asInstanceOf[HasVal[IN]], adder.asInstanceOf[Y => IN => Unit])
      })
      slicer
    }
    input.newIsomorphicVector(cellBuilder)
  }

  def all():VectTerm[K, Y#OUT] = buildSliced(ReduceType.CUMULATIVE)
  def last():VectTerm[K, Y#OUT] = buildSliced(ReduceType.LAST)
}
