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
    val collapsed = new HasVal[VectorStream[K,X]] with types.MFunc {
      def value = input
      def trigger = this
      def calculate() = true
    }
    // link the 'collapsed' function up so it is added as a listener to every function in the source vector
    new ChainedVector[K, types.MFunc, types.MFunc](input, eval.env) {
      def newCell(i: Int, key: K) = {
        val cellTrigger: EventGraphObject = input.getTrigger(i)
        eval.bind(cellTrigger, collapsed)
        collapsed
      }

      def get(i: Int) = getAt(i)
    }
    return new MacroTerm[VectorStream[K,X]](eval)(collapsed)
  }


  def map[Y](f:X=>Y):VectTerm[K,Y] = {
    class FuncApply(sourceIndex:Int) extends AbsFunc[X,Y] {
      def calculate():Boolean = {
        val x: X = input.get(sourceIndex)
        value = f(x)
        return true
      }
    }

    val output:VectorStream[K, Y] = new ChainedVector[K, Func[X,Y], Y](input, eval.env) {
      def newCell(i: Int, key: K) :Func[X,Y] = {
        val cellFunc: FuncApply = new FuncApply(i)
        val sourceTrigger: EventGraphObject = input.getTrigger(i)
        eval.bind(sourceTrigger, cellFunc)
        return cellFunc
      }

      def get(i: Int) = getAt(i).value
    }
    return new VectTerm[K, Y](eval)(output)
  }

  def fold_all_noMacro[Y <: Reduce[X]](reduceBuilder:() => Y):VectTerm[K,Y] = {
    class FuncApply(sourceIndex:Int) extends AbsFunc[X,Y] {
      value = reduceBuilder.apply()
      def calculate():Boolean = {
        val x: X = input.get(sourceIndex)
        value.add(x)
        return true
      }
    }

    val output:VectorStream[K, Y] = new ChainedVector[K, Func[X,Y], Y](input, eval.env) {
      def newCell(i: Int, key: K) :Func[X,Y] = {
        val cellFunc: FuncApply = new FuncApply(i)
        val sourceTrigger: EventGraphObject = input.getTrigger(i)
        eval.bind(sourceTrigger, cellFunc)
        return cellFunc
      }

      def get(i: Int) = getAt(i).value
    }
    return new VectTerm[K, Y](eval)(output)
  }


  //  def reduce[Y <: Reduce[X]](y:Y, window:Window = null):VectTerm[K,Y] = ???
}
