package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scala.reflect.ClassTag
import java.util.logging.Logger
import scespet.core.VectorStream.ReshapeSignal

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:10
 * To change this template use File | Settings | File Templates.
 */
class VectTerm[K,X](val env:types.Env)(val input:VectorStream[K,X]) extends BucketVectTerm[K,X] {
  import scala.reflect.macros.Context
  import scala.collection.JavaConverters._

  import scala.language.experimental.macros

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


  private class ChainedCell[C]() extends UpdatingHasVal[C] {
    var source: HasValue[C] = _
    var value = null.asInstanceOf[C]

    def calculate() = {
      value = source.value
      true
    }

    def bindTo(newSource: HasValue[C]) {
      this.source = newSource
      env.addListener(newSource, this)
    }
  }

  /**
   * demultiplex operation. Want to think more about other facilities on this line
   * how do we really want to do a demean-like operation to a vector?
   * This approach allows us to treat it like a 'reduce' on a stream of vectors, but is that really the best way?
   *
   * TODO: not happy with the return type, I think it should maybe be MacroTerm[Map[K,X]] ?
   * @return
   */
  def collapse():MacroTerm[VectorStream[K,X]] = {
    val collapsed = new UpdatingHasVal[VectorStream[K,X]] {
      val value = input
      def calculate() = true
    }
    val cellBuilder = (index:Int, key:K) => collapsed
    // do the normal chained vector wiring, but don't return it
    newIsomorphicVector(cellBuilder)
    return new MacroTerm[VectorStream[K, X]](env)(collapsed)
  }

  def map[Y](f:X=>Y):VectTerm[K,Y] = {
    class MapCell(index:Int) extends UpdatingHasVal[Y] {
//      var value = f(input.get(index)) // NOT NEEDED, as we generate a cell in response to an event, we auto-call calculate on init
      var value:Y = _
      def calculate() = {
        var in = input.get(index)
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
      var value:Y = _
      def calculate() = {
        val inputVal = input.get(index)
        val oy = reflect.classTag[Y].unapply(inputVal)
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

  def fold_all[Y <: Reduce[X]](reduceBuilder : => Y):VectTerm[K,Y] = {
    val cellBuilder = (index:Int, key:K) => new UpdatingHasVal[Y] {
      val value = reduceBuilder
      def calculate():Boolean = {
        val x: X = input.get(index)
        value.add(x)
        return true
      }
    }
    return newIsomorphicVector(cellBuilder)
  }

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
   */
  def valueSet[Y]( expand: (X=>TraversableOnce[Y]) = ( (x:X) => Traversable(x.asInstanceOf[Y]) ) ):VectTerm[Y,Y] = {
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
   * todo: maybe this should be called 'takef', or 'joinf' ? (i.e. we're 'taking' or 'joining' with a function?)
   *@param cellFromKey
   * @tparam Y
   * @return
   */
  def joinf[Y]( cellFromKey:K=>HasVal[Y] ):VectTerm[K,Y] = {
    val output: VectorStream[K, Y] = new ChainedVector[K, EventGraphObject, Y](input, env) {
      def newCell(i: Int, key: K) = cellFromKey(key)
    }
    return new VectTerm[K, Y](env)(output)
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

  def reduceNoMacro[Y <: Reduce[X]](newBFunc: => Y):BucketBuilderVect[K, X, Y] = new BucketBuilderVectImpl[K, X,Y](() => newBFunc, VectTerm.this, env)

  def reduce[Y <: Reduce[X]](bucketFunc:Y):BucketBuilderVect[K, X, Y] = macro BucketMacro.bucket2MacroVect[K,X,Y]

  def newBucketBuilder[B](newB: () => B): BucketBuilderVect[K, X, B] = {
    type Y = B with Reduce[X]
    val reduceGenerator: ()=>Y = newB.asInstanceOf[() => Y]
    //    val reduceGenerator = newB
    var aY = reduceGenerator.apply()
    println("Reducer in VectTerm generates "+aY)
    //    new BucketBuilderImpl[X, B](reduceGenerator, input, eval).asInstanceOf[BucketBuilder[T]]
    return new BucketBuilderVectImpl[K, X, Y](reduceGenerator, new VectTerm[K, X](env)(input), env).asInstanceOf[BucketBuilderVect[K, X, B]]
  }

}
