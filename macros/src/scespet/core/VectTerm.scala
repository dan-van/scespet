package scespet.core

import stub.gsa.esg.mekon.core.EventGraphObject

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:10
 * To change this template use File | Settings | File Templates.
 */
class VectTerm[K,X](val eval:FuncCollector)(input:VectorStream[K,X]) {

//  def mapEach[Y](createFunc: (K) => Func[X,Y]): VectorExpr[K, Y] = {
//    val funcVector = new BoundFunctionVector[K, X, Func[X,Y], Y](createFunc, source, collector)
//    funcVector.setSource(source)
//    return new VectorExpr[K, Y](funcVector)
//  }

//  def mapf[Y](f:Func[X,Y]): VectorExpr[K, Y] = {
//    val createFunc: (K) => Func[X,Y] = (_) => {f.getClass.newInstance()}
//    return mapEach(createFunc)
//  }

  /**
   * demultiplex operation. Want to think more about other facilities on this line
   * how do we really want to do a demean-like operation to a vector?
   * is it a custom reduction, or is this a good approach
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

  class FuncApplyCell[Y](f: X=>Y, index:Int, key:K) extends UpdatingHasVal[Y] {
    var value = null.asInstanceOf[Y]

    // initialise value
    calculate()

    def calculate():Boolean = {
      val x: X = input.get(index)
      value = f(x)
      return true
    }
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

  //  def reduce[Y <: Reduce[X]](y:Y, window:Window = null):VectTerm[K,Y] = ???
}
