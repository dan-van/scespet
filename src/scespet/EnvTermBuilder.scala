package scespet

import core._
import core.types
import gsa.esg.mekon.core.EventSource
import scespet.core.SliceCellLifecycle.{CellSliceCellLifecycle, BucketCellLifecycle}
import scespet.core.VectorStream.ReshapeSignal
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

  /**
   * A VectTerm that generates new cells on demand according to the given K=>HasVal[X] function
   * e.g. you could use this to present an existing factory as a Vector, allowing easy joins
   *
   * @param gen
   * @tparam K
   * @tparam X
   * @return
   */
  def lazyVect[K, X](gen:K => HasVal[X]) :VectTerm[K, X] = {
    new VectTerm[K,X](env)(new AbstractVectorStream[K, X](env) {

      override def newCell(i: Int, key: K): HasValue[X] = {
        gen(key)
      }

      val getNewColumnTrigger = new ReshapeSignal(env)

      val isInitialised: Boolean = true

      override def indexOf(key: K): Int = {
        val i = super.indexOf(key)
        if (i < 0) {
          add(key)
          getSize - 1
        } else {
          i
        }
      }
    })
  }

  def streamOf3[Y <: MFunc, OUT](newCellFunc: => Y)(implicit aggOut:AggOut[Y,OUT]) : PartialBuiltSlicedBucket[Y, OUT] = {
    //    if (data.isInstanceOf[EventSource]) {
    //      env.registerEventSource(data.asInstanceOf[EventSource])
    //    }
    val cellLifeCycle:SliceCellLifecycle[Y] = new CellSliceCellLifecycle[Int, Y](() => newCellFunc)
    return new PartialBuiltSlicedBucket[Y, OUT](aggOut, cellLifeCycle, env)
  }

  def streamOf2[Y <: Bucket, OUT](newCellFunc: => Y)(implicit aggOut:AggOut[Y,OUT]) : PartialBuiltSlicedBucket[Y, OUT] = {
    //    if (data.isInstanceOf[EventSource]) {
    //      env.registerEventSource(data.asInstanceOf[EventSource])
    //    }
    val cellLifeCycle:SliceCellLifecycle[Y] = new BucketCellLifecycle[Y] {
      override def newCell(): Y = newCellFunc
    }
    return new PartialBuiltSlicedBucket[Y, OUT](aggOut, cellLifeCycle, env)
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
  // this implicit should be somewhere else. I want it generally available to enable mapping from standard Mekon streams into Scesspet streams
  implicit def eventObjectToHasVal[X <: types.EventGraphObject](evtObj:X) :HasVal[X] = new IsVal(evtObj)
}
