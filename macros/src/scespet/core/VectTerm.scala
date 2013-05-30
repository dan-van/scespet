package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scala.reflect.ClassTag

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
      def calculate() = {value = f(input.get(index)); true}
    }
    val cellBuilder = (index:Int, key:K) => new MapCell(index)
//    val cellBuilder = (index:Int, key:K) => {
//      val cellUpdateFunc = () => { f(input.get(index)) }
//      val initial = cellUpdateFunc()
//      new Generator[Y](initial, cellUpdateFunc)
//    }
    return newIsomorphicVector(cellBuilder)
  }

  def fold_all2[Y <: Reduce[X]](reduceBuilder : => Y):VectTerm[K,Y] = {
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

  def fold_all_noMacro[Y <: Reduce[X]](reduceBuilder:() => Y):VectTerm[K,Y] = {
    val cellBuilder = (index:Int, key:K) => new UpdatingHasVal[Y] {
      val value = reduceBuilder.apply()
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
        val sourceTrigger: EventGraphObject = input.getTrigger(i)
        env.addListener(sourceTrigger, cellFunc)
        // initialise the cell
        cellFunc.calculate()
        return cellFunc
      }

      def get(i: Int) = getAt(i).value
    }
    return new VectTerm[K, Y](env)(output)
  }

  /**
   * used to build a set from the values in a vector
   * the new vector acts like a set (key == value), generated values are folded into it.
   */
  def valueSet[Y : ClassTag]( expand: (X=>TraversableOnce[Y]) = ( (x:X) => Traversable(x.asInstanceOf[Y]) ) ):VectTerm[Y,Y] = {
    val initial = collection.mutable.Set[Y]()
    for (x <- input.getValues.asScala; y <- expand(x)) {
      initial += y
    }
    val typeY = reflect.classTag[Y]
    val javaClass : Class[Y] = typeY.runtimeClass.asInstanceOf[Class[Y]]
    val flattenedSet = new MutableVector[Y](javaClass, initial.toIterable.asJava, env) with types.MFunc {
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
   * derive a new vector with the same keys as this one, but different values.
   * ,aybe this should be called 'takef', or 'joinf' ? (i.e. we're 'taking' or 'joining' with a function?)
   *@param cellFromKey
   * @tparam Y
   * @return
   */
  def mapk[Y]( cellFromKey:K=>HasVal[Y] ):VectTerm[K,Y] = {
    val output: VectorStream[K, Y] = new ChainedVector[K, HasVal[Y], Y](input, env) {
      def newCell(i: Int, key: K): HasVal[Y] = cellFromKey(key)
      def get(i: Int) = getAt(i).value
    }
    return new VectTerm[K, Y](env)(output)
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
