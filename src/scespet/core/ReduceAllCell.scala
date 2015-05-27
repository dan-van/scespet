package scespet.core

/**
 * Created by danvan on 16/04/2014.
 */
// todo: isn't this identical to the single Term reduceAll implementation?
class ReduceAllCell[K, X, Y <: Agg[X]](env:types.Env, input:VectorStream[K,X], index:Int, key:K, reduceBuilder : K => Y, reduceType:ReduceType) extends UpdatingHasVal[Y#OUT] {
  val termination = env.getTerminationEvent
  env.addListener(termination, this)

  var value:Y#OUT = null.asInstanceOf[Y#OUT]
  val reduction:Y = reduceBuilder(key)
  val fireEachEvent = reduceType == ReduceType.CUMULATIVE

  def calculate():Boolean = {
    if (env.hasChanged(termination)) {
      value = reduction.asInstanceOf[Y].value
      initialised = true
      true
    } else {
      val x: X = input.get(index)
      reduction.add(x)
      fireEachEvent
    }
  }
}

