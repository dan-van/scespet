package scespet

import core._
import core.types
import gsa.esg.mekon.core.EventSource
import scespet.core.types.MFunc

/**
 * This needs to have 'init(env)' called before you can use it.
 * Why not a constructor!? Because I'm experimenting with the following approach that allows a 'program' to be encapsulated with access to all utilities
 * and implicit env variables as needed, where the env is provided later:
 *
 * val myProg = new EnvTermBuilder {
 *   val myStream1 = asStream( someEventGraphObject )
 *   ...
 * }
 *
 * which allows the "myProg" to now be run in an execution engine/context of choice.
 * e.g. remotely, within an existing environment, with Mekon (proprietary) or the dumb scespet.core.SimpleEvaluator
 *
 */
class EnvTermBuilder() extends DelayedInit {
  implicit var env:types.Env = _

  private var initBody = List[()=>Unit]()

  override def delayedInit(func: => Unit) {
    // first one is my own init - do it!
    if (this.initBody == null) {
      func
    } else {
      val funcReference = () => {func;}
      initBody :+= funcReference
    }
  }

  def init(env:types.Env) {
    this.env = env
    for (b <- initBody) b.apply()
  }

  def asStream[X](data: HasVal[X]) : MacroTerm[X] = {
    if (data.isInstanceOf[EventSource]) {
      env.registerEventSource(data.asInstanceOf[EventSource])
    }
    return new MacroTerm[X](env)(data)
  }

  def query[X](newHasVal :(types.Env) => HasVal[X]) : Term[X] = {
    val hasVal = newHasVal(env)
    asStream(hasVal)
  }

  def asVector[X](elements:Iterable[X]) :VectTerm[X,X] = {
    import scala.collection.JavaConverters._
    new VectTerm[X,X](env)(new MutableVector(elements.asJava, env))
  }

  def streamOf[X <: Bucket](data: X) : PartialAggOrAcc[X, _] = {
//    if (data.isInstanceOf[EventSource]) {
//      env.registerEventSource(data.asInstanceOf[EventSource])
//    }
//    return new PartialAggOrAcc[X, ](env)(data)
    ???
  }


//  def reduce[B <: types.MFunc](aggregateBuilder: => B) :ReduceBuilder[B] = {
//    new ReduceBuilder[B](aggregateBuilder)
//  }

}

//class ReduceBuilder[B <: types.MFunc](aggregateBuilder : => B) {
//  val bBuilder = aggregateBuilder
//
//  def join[X](MultiTerm[])
//}

object EnvTermBuilder {
  def apply(env:types.Env):EnvTermBuilder = {
    val builder = new EnvTermBuilder
    builder.init(env)
    builder
  }
  
  implicit def eventObjectToHasVal[X <: types.EventGraphObject](evtObj:X) :HasVal[X] = new IsVal(evtObj)
}
