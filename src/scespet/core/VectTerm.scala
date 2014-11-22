package scespet.core

import java.util.logging.Logger

import gsa.esg.mekon.core.{Environment, EventGraphObject}
import scespet.core.SliceCellLifecycle.{CellSliceCellLifecycle, AggSliceCellLifecycle, BucketCellLifecycleImpl}
import scespet.core.VectorStream.ReshapeSignal
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag


import scespet.util._
import scespet.util.SliceAlign._
import scala.Some

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:10
 * To change this template use File | Settings | File Templates.
 */
class VectTerm[K,X](val env:types.Env)(val input:VectorStream[K,X]) extends MultiTerm[K,X] {
  val logger = Logger.getLogger(classOf[VectTerm[_,_]].getName)
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
    val index: Int = input.indexOf(k)
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
      // attach a function that waits for the key to be present in the source vector, and then wires and
      // initialises the next cell.
      env.addListener(newColumns, new types.MFunc {
        var searchedUpTo = input.getSize
        def calculate():Boolean = {
          for (i <- searchedUpTo to input.getSize - 1) {
            if (input.getKey(i) == k) {
              val inputCell = input.getValueHolder(i)
              valueHolder.bindTo(inputCell)
              if (inputCell.initialised()) {
                // NODEPLOY - I think this will be caught by my new initialisation semantics?
                logger.info("Encountered real cell for speculatively build chained cell on key: "+k+" the input is initialised")
//                valueHolder.calculate() // this is possibly overkill - I'll write the tests then remove this line, but I like the idea of it being ready as soon as we respond to the new column
//
//                env.wakeupThisCycle(valueHolder)  // damn - this is firing before fireAfterChangingListeners catches the underlying cell
                // we have just connected an existing listener chain to a new input, that is already initialised.
                // We want to propagate this new value down the listener chain
                // however, I'm unsure whether this should be wakeupThisCycle, or fireAfterChangingListeners?
                // wakeup this cycle makes more sense, except for the fact that I don't currently apply structure modifications immediately
                // therefore we're stuck with fireAfterChangingListeners, but then that means the event is in a different
                // event cycle to the current firing context
                env.fireAfterChangingListeners(valueHolder)
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
      if (newSource.initialised()) {
        calculate() // propagate initialisation
      }
    }

    override def toString: String = {
      "ChainedCell{"+source+"}"
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

      if (input.getValueHolder(index).initialised()) {
        // calculate doesn't depend on hasChanged, so we can use the same logic for initilisation
        initialised = calculate()
      } else {
        // NODEPLOY
        println(s"vect.map inputcell is not initialised")
      }

      def calculate() = {
        // note: we're relying on not using env.hasChanged here, because we use this method as initialisation in the constructor
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
    val cellBuilder = (index:Int, key:K) => {
      val newCell = new MapCell(index)
      if (input.getValueHolder(index).initialised()) {
        // initialise it if the input is already initialised
        newCell.calculate()
      }
      newCell
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


  private [core] def newIsomorphicVector[Y](cellBuilder: DerivedVector[K, Y]): VectTerm[K, Y] = {
    val newVect = newIsomorphicVector[Y](cellBuilder.newCell _)
    for (dep <- cellBuilder.dependsOn) {
      // really we just need an ordering that dep 'comes-before' we try to build new cells
      // however in my simplified version of the graph walk, I've not supported that
      env.addListener(dep, newVect.input.getNewColumnTrigger)
    }
    newVect
  }

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
          println("NODEPLOY - didn't expect hasChanged=true, but initialised=false for "+sourceCell)
        }
        val oldIsInitialised = hasInputValue || hasChanged
        if (oldIsInitialised && !cellFunc.initialised) {
//          warn instead? Is it jus the decision of cellFunc?
//          throw new UnsupportedOperationException("newCell construction needs to yield an initialised value, given that the input cell is initialised:\n" +
          logger.warning("newCell constructed non-initialised, but input is initialised. We may not want to do this:\n" +
            "Input = "+sourceCell+"\n" +
            "newCell = "+cellFunc+" from\n" +
            "cellBuilder="+cellBuilder+" on key="+key)
//          env.wakeupThisCycle(cellFunc)
          // NODEPLOY - this is not good. we don't know if cellFunc will guard itself with hasChanged
//          val hasInitialOutput = cellFunc.calculate()
//
//          if (hasInitialOutput && ! cellFunc.initialised) {
//            throw new AssertionError("Cell should have been initialised by calculate: "+cellFunc+" for key "+key)
//          }
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

  def toValueSet :VectTerm[X,X] = {
    toValueSet[X](x => Traversable(x))
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
  def toValueSet[Y]( expand: (X=>TraversableOnce[Y])):VectTerm[Y,Y] = {
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
          val inputCell = input.getValueHolder(i)
          if (inputCell.initialised()) {
            // expand x and add elements
            val x = inputCell.value()
            this.addAll(expand(x).toIterable.asJava)
          } else {
            // NODEPLOY - check this, test it somehow
            println("cell not initialised yet: "+inputCell)
          }
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

  /**
   * a multiplex operation. Opposite of MacroTerm.by(f)
   */
  def toStream(): MacroTerm[(K,X)] = {
    val multiplexed = new VectorToStream[K,X](input, env) 
    new MacroTerm[(K,X)](env)( multiplexed ) 
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


  // THINK: this could be special cased to be faster
//  def reduce[Y <: Agg[X]](newBFunc: K => Y):VectTerm[K, Y#OUT] = group[Any](null, AFTER)(SliceTriggerSpec.TERMINATION).reduce(newBFunc)

  def reduce[Y, O](newBFunc: => Y)(implicit adder:Y => CellAdder[X], yOut :AggOut[Y, O]):VectTerm[K, O] = reduce[Y, O]((k:K) => newBFunc)(adder, yOut)
  // THINK: this could be special cased to be faster
  def reduce[Y, O](newBFunc: K => Y)(implicit adder:Y => CellAdder[X], yOut :AggOut[Y, O]):VectTerm[K, O] = group[Any](null, AFTER)(SliceTriggerSpec.TERMINATION.asVectSliceSpec).reduce(newBFunc)(adder, yOut)

  def scan[Y, O](newBFunc: => Y)(implicit adder:Y => CellAdder[X], yOut :AggOut[Y, O]):VectTerm[K, O] = scan[Y, O]((k:K) => newBFunc)(adder,yOut)
  // THINK: this could be special cased to be faster
  def scan[Y, O](newBFunc: K => Y)(implicit adder:Y => CellAdder[X], yOut :AggOut[Y, O]) :VectTerm[K,O] = group[Null](null, AFTER)(SliceTriggerSpec.NULL.asVectSliceSpec).scan(newBFunc)(adder,yOut)

  def bindTo[B <: Bucket, OUT](newBFunc: => B)(adder: B => X => Unit)(implicit aggOut: AggOut[B, OUT]) :PartialBuiltSlicedVectBucket[K, B, OUT] = bindTo[B, OUT]((k:K) => newBFunc)(adder)(aggOut)

  // TODO: how about bindTo(vect[K,B]) ?
  def bindTo[B <: Bucket, OUT](newBFunc: K => B)(adder: B => X => Unit)(implicit aggOut: AggOut[B, OUT]) :PartialBuiltSlicedVectBucket[K, B, OUT] = {
    val keyToCellLifecycle = new KeyToSliceCellLifecycle[K, B] {
      override def lifeCycleForKey(k: K): SliceCellLifecycle[B] = new BucketCellLifecycleImpl[B](newBFunc(k))

      override def isCellListenable: Boolean = true
    }
    new PartialBuiltSlicedVectBucket[K, B, OUT](this, aggOut, keyToCellLifecycle, env).bind(this)(adder)
  }

  def group[S](sliceSpec:S, triggerAlign:SliceAlign = AFTER)(implicit ev:VectSliceTriggerSpec[S]) :GroupedVectTerm[K, X] = {
    val uncollapsed = new UncollapsedVectGroupWithTrigger[S, K, X](input, sliceSpec, triggerAlign, env, ev)
    val grouped = new GroupedVectTerm[K, X](this, uncollapsed, env)
    grouped
  }

  // THINK: may want to go with TypeClasses here?
  def window(windowVect:VectTerm[K, Boolean]):GroupedVectTerm[K, X] = {
    window((k:K) => windowVect(k), Set(windowVect.input.getNewColumnTrigger))
  }

  def window(windowStream:HasVal[Boolean]):GroupedVectTerm[K, X] = window(_ => windowStream)

  def window(keyToWindow:K => HasVal[Boolean]):GroupedVectTerm[K, X] = {
    window(keyToWindow, Set())
  }

  private def window(keyToWindow:K => HasVal[Boolean], sliceStateDependencies :Set[EventGraphObject]) :GroupedVectTerm[K, X] = {
      val uncollapsed = new UncollapsedVectGroup[K, X] {
        override def applyNew[A, OUT](keyedLifecycle: KeyToSliceCellLifecycle[K, A], adder: (A) => CellAdder[X], cellOut: AggOut[A, OUT], reduceType: ReduceType): DerivedVector[K, OUT] = {
          new DerivedVector[K, OUT] {
            override def newCell(i: Int, k: K): UpdatingHasVal[OUT] = {
              val sourceCell = input.getValueHolder(i)
              val lifecycle = keyedLifecycle.lifeCycleForKey(k)
              val windowStream = keyToWindow(k)
              val outputCell:UpdatingHasVal[OUT] = new WindowedReduce[X, A, OUT](sourceCell, adder, cellOut, windowStream, lifecycle, reduceType, env)
              outputCell
            }

            override def dependsOn: Set[EventGraphObject] = sliceStateDependencies
          }
        }

        override def applyB[B <: Bucket, OUT](keyedLifecycle: KeyToSliceCellLifecycle[K, B], cellOut: AggOut[B, OUT], reduceType: ReduceType): DerivedVector[K, OUT] = {
        new DerivedVector[K, OUT] {
          override def newCell(i: Int, k: K): UpdatingHasVal[OUT] = {
            val sourceCell = input.getValueHolder(i)
            val lifecycle = keyedLifecycle.lifeCycleForKey(k)
            val windowStream = keyToWindow(k)
            val outputCell:UpdatingHasVal[OUT] = reduceType match {
              case ReduceType.CUMULATIVE => new WindowedBucket_Continuous[B, OUT](cellOut, windowStream, lifecycle, env)
              case ReduceType.LAST => new WindowedBucket_LastValue[B, OUT](cellOut, windowStream, lifecycle, env)
            }
            outputCell
          }

          override def dependsOn: Set[EventGraphObject] = sliceStateDependencies
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


class VectCellLifecycle[K, X, Y](newCellF: K => Y) extends KeyToSliceCellLifecycle[K,Y]{
  override def lifeCycleForKey(k: K): SliceCellLifecycle[Y] = new CellSliceCellLifecycle[X, Y]( () => newCellF(k) )

  override def isCellListenable: Boolean = false
}

// NODEPLOY delete this

class VectAggLifecycle[K, X, Y <: Agg[X]](newCellF: K => Y) extends KeyToSliceCellLifecycle[K,Y]{
  override def lifeCycleForKey(k: K): SliceCellLifecycle[Y] = new AggSliceCellLifecycle[X, Y]( () => newCellF(k) )

  override def isCellListenable: Boolean = false
}

class GroupedVectTerm[K, X](val input:VectTerm[K,X], val uncollapsedGroup: UncollapsedVectGroup[K, X], val env:types.Env) {
//  var orderDepends = Seq[VectTerm[K,_]]()
//  private [core] def newCellsDependOn(prerequisite :EventGraphObject) = {
//    orderDepends :+= term
//  }
  def last :VectTerm[K,X] = {
    val ident :K=>MutableValue[X] = (k:K) => {val idx = input.input.indexOf(k); new MutableValue(input.input.get(idx))}
    reduce(ident)
  }

  def reduce[Y, O](newBFunc: => Y)(implicit adder:Y => CellAdder[X], yOut :AggOut[Y, O]) :VectTerm[K,O] = {
    collapse((k:K) => newBFunc, adder, yOut, ReduceType.LAST)
  }
  def reduce[Y, O](newBFunc: K => Y)(implicit adder:Y => CellAdder[X], yOut :AggOut[Y, O]) :VectTerm[K,O] = {
    collapse(newBFunc, adder, yOut, ReduceType.LAST)
  }
  def scan[Y, O](newBFunc: => Y)(implicit adder:Y => CellAdder[X], yOut :AggOut[Y, O]) :VectTerm[K,O] = {
    collapse((k:K) => newBFunc, adder, yOut, ReduceType.CUMULATIVE)
  }
  def scan[Y, O](newBFunc: K => Y)(implicit adder:Y => CellAdder[X], yOut :AggOut[Y, O]) :VectTerm[K,O] = {
    collapse(newBFunc, adder, yOut, ReduceType.CUMULATIVE)
  }

  private def collapse[Y, O](newBFunc: K => Y, adder:Y => CellAdder[X], yOut :AggOut[Y, O], reduceType:ReduceType) :VectTerm[K,O] = {
    val lifecycle :VectCellLifecycle[K, X, Y] = new VectCellLifecycle[K, X, Y](newBFunc)
    val cellBuilder = uncollapsedGroup.applyNew[Y, O](lifecycle, adder, yOut, reduceType)
    val newVect = input.newIsomorphicVector[O](cellBuilder)
    return newVect
  }
}

trait UncollapsedVectGroup[K, IN] {
  def applyB[B <: Bucket, OUT](lifecycle:KeyToSliceCellLifecycle[K, B], cellOut: AggOut[B, OUT], reduceType:ReduceType) :DerivedVector[K, OUT]
  def applyNew[A, OUT](lifecycle: KeyToSliceCellLifecycle[K, A], adder:A => CellAdder[IN], cellOut:AggOut[A,OUT], reduceType:ReduceType): DerivedVector[K, OUT]
}

trait DerivedVector[K, OUT] {
  def newCell(i:Int, k:K):UpdatingHasVal[OUT]
  def dependsOn:Set[EventGraphObject]
}

class UncollapsedVectGroupWithTrigger[S, K, IN](inputVector:VectorStream[K, IN], sliceSpec: S, triggerAlign:SliceAlign, env:types.Env, ev: VectSliceTriggerSpec[S]) extends UncollapsedVectGroup[K, IN] {
  val newCellDependencies = ev.newCellPrerequisites(sliceSpec)

  override def applyNew[A, OUT](lifecycle: KeyToSliceCellLifecycle[K, A], adder: (A) => CellAdder[IN], cellOut: AggOut[A, OUT], reduceType: ReduceType): DerivedVector[K, OUT] = {
    new DerivedVector[K, OUT] {
      def newCell(i: Int, k: K):UpdatingHasVal[OUT] = {
        val sourceCell = inputVector.getValueHolder(i)
        val cellLifecycle = lifecycle.lifeCycleForKey(k)
        val sliceSpecEv = ev.toTriggerSpec(k, sliceSpec)
        println("New sliced reduce for key: " + k + " from source: " + inputVector)
        new SlicedReduce[S, IN, A, OUT](sourceCell, adder, cellOut, sliceSpec, triggerAlign == BEFORE, cellLifecycle, reduceType, env, sliceSpecEv, exposeInitialValue = true)
      }
      // NODEPLOY - if I could ask lifeCycle if its cells
      override def dependsOn: Set[EventGraphObject] = newCellDependencies
    }
  }

  def applyB[B <: Bucket, OUT](lifecycle: KeyToSliceCellLifecycle[K, B], cellOut: AggOut[B, OUT], reduceType: ReduceType): DerivedVector[K, OUT] = {
    new DerivedVector[K, OUT] {
      override def newCell(i: Int, k: K): UpdatingHasVal[OUT] = {
        val sourceCell = inputVector.getValueHolder(i)
        val cellLifecycle = lifecycle.lifeCycleForKey(k)
        val sliceSpecEv = ev.toTriggerSpec(k, sliceSpec)
        triggerAlign match {
          case BEFORE => {
              new SliceBeforeBucket[S, B, OUT](cellOut, sliceSpec, cellLifecycle, reduceType, env, sliceSpecEv, exposeInitialValue = true)
          }
          case AFTER => {
              new SliceAfterBucket[S, B, OUT](cellOut, sliceSpec, cellLifecycle, reduceType, env, sliceSpecEv, exposeInitialValue = true)
          }
          case _ => throw new IllegalArgumentException(String.valueOf(triggerAlign))
        }
      }

      override def dependsOn: Set[EventGraphObject] = newCellDependencies
    }
  }
}

// NODEPLOY this should be a Term that also supports partitioning operations
class PartialBuiltSlicedVectBucket[K, Y <: Bucket, OUT](input:VectTerm[K, _], yOut :AggOut[Y, OUT], val keyCellLifecycle: KeyToSliceCellLifecycle[K, Y], val env:Environment) {
  var bindings = List[(VectTerm[K, _], (_ => _ => Unit))]()

  private lazy val scanAllTerm: VectTerm[K, OUT] = {
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


  def last(): VectTerm[K, OUT] = {
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
  def all(): VectTerm[K, OUT] = scanAllTerm

  def bind[S](stream: VectTerm[K, S])(adder: Y => S => Unit): PartialBuiltSlicedVectBucket[K, Y, OUT] = {
    bindings :+=(stream, adder)
    this
  }

  // NODEPLOY - I think this would be better named as 'reset', once you already have a stream->reducer binding, talking about grouping is confusing.
  //NODEPLOY - think:
  // CellLifecycle creates a new cell at beginning of stream, then multiple calls to close bucket after a slice
  // this avoids needing a new slice trigger definition each slice.
  def reset[S](sliceSpec: S, triggerAlign: SliceAlign = AFTER)(implicit ev: SliceTriggerSpec[S]):PartialGroupedVectBucketStream[K, S, Y, OUT] = {
    new PartialGroupedVectBucketStream[K, S, Y, OUT](input, triggerAlign, keyCellLifecycle, yOut, bindings, sliceSpec, ev, env)
  }
}

class PartialGroupedVectBucketStream[K, S, Y <: Bucket, OUT](input:VectTerm[K,_],
                                                        triggerAlign:SliceAlign,
                                                        keyCellLifecycle:KeyToSliceCellLifecycle[K, Y],
                                                        cellOut: AggOut[Y, OUT],
                                                        bindings :List[(VectTerm[K, _], (_ => _ => Unit))],
                                                        sliceSpec:S, ev:SliceTriggerSpec[S], env:types.Env) {
  private def buildSliced(reduceType:ReduceType) :VectTerm[K, OUT] = {
    val cellBuilder = (i:Int, k:K) => {
      val cellLifecycle = keyCellLifecycle.lifeCycleForKey(k)
      val slicer = triggerAlign match {
        case BEFORE => new SliceBeforeBucket[S, Y, OUT](cellOut, sliceSpec, cellLifecycle, reduceType, env, ev, exposeInitialValue = true)
        case AFTER => new SliceAfterBucket[S, Y, OUT](cellOut, sliceSpec, cellLifecycle, reduceType, env, ev, exposeInitialValue = true)
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

  def all():VectTerm[K, OUT] = buildSliced(ReduceType.CUMULATIVE)
  def last():VectTerm[K, OUT] = buildSliced(ReduceType.LAST)
}
