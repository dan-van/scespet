package scespet.core

import stub.gsa.esg.mekon.core.EventGraphObject

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:10
 * To change this template use File | Settings | File Templates.
 */
class VectTerm[K,X](val eval:FuncCollector)(val input:VectorStream[K,X]) {
  /**
   * demultiplex operation. Want to think more about other facilities on this line
   * how do we really want to do a demean-like operation to a vector?
   * This approach allows us to treat it like a 'reduce' on a stream of vectors, but is that really the best way?
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
    return new MacroTerm[VectorStream[K, X]](eval)(collapsed)
  }

  def map[Y](f:X=>Y):VectTerm[K,Y] = {
    class MapCell(index:Int) extends UpdatingHasVal[Y] {
      var value = f(input.get(index))
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
    val output: VectorStream[K, Y] = new ChainedVector[K, UpdatingHasVal[Y], Y](input, eval.env) {
      def newCell(i: Int, key: K): UpdatingHasVal[Y] = {
        val cellFunc: UpdatingHasVal[Y] = cellBuilder.apply(i, key)
        val sourceTrigger: EventGraphObject = input.getTrigger(i)
        eval.bind(sourceTrigger, cellFunc)
        return cellFunc
      }

      def get(i: Int) = getAt(i).value
    }
    return new VectTerm[K, Y](eval)(output)
  }


  def mapk[Y]( cellFromKey:K=>HasVal[Y] ):VectTerm[K,Y] = {
    val output: VectorStream[K, Y] = new ChainedVector[K, HasVal[Y], Y](input, eval.env) {
      def newCell(i: Int, key: K): HasVal[Y] = cellFromKey(key)
      def get(i: Int) = getAt(i).value
    }
    return new VectTerm[K, Y](eval)(output)
  }

  def reduceNoMacro[Y <: Reduce[X]](newBFunc:() => Y):BucketBuilderVectImpl[K, X, Y] = new BucketBuilderVectImpl[K, X,Y](newBFunc, VectTerm.this, eval)

  //  def reduce[Y <: Reduce[X]](y:Y, window:Window = null):VectTerm[K,Y] = ???
}
