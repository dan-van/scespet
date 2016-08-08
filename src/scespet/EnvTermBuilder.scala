package scespet

import scespet.core._
import gsa.esg.mekon.core.{Environment, EventSource}
import scespet.core.SliceCellLifecycle.{MutableBucketLifecycle, CellSliceCellLifecycle}
import scespet.core.VectorStream.ReshapeSignal
import scespet.core.types.MFunc
import scespet.util.SliceAlign
import scespet.util.SliceAlign._

import scala.reflect.ClassTag

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
    if (data.getTrigger.isInstanceOf[EventSource]) {
      env.registerEventSource(data.getTrigger.asInstanceOf[EventSource])
    }
    return new MacroTerm[X](env)(data)
  }

  @Deprecated // rename to asStream?
  def query[X](newHasVal :(types.Env) => HasVal[X]) : Term[X] = {
    val hasVal = newHasVal(env)
    asStream(hasVal)
  }

  /**
    * this is a richer stream build that allows the stream definition to be an MFunc with setters bound to other streams, and possible slicing operations
    * {@see asStream} is equivalent (though more efficient) to buildStream(newCellFunc).all()
    * PONDER: I think I could evaporate this implementation detail and have a single buildStream method.
    *
    * @param newCellFunc
    * @param aggOut
    * @param yType
    * @tparam Y
    * @tparam OUT
    * @return
    */
  def buildStream[Y <: MFunc, OUT](newCellFunc: => Y)(implicit aggOut:AggOut[Y, OUT], yType:ClassTag[Y]) : ResettableBucketStreamBuild[Y, OUT] = {
    val lifecycle = scespet.core.SliceCellLifecycle.buildLifecycle(() => newCellFunc, yType)
    new ResettableBucketStreamBuild[Y, OUT](aggOut, lifecycle, yType, env)
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

      val getNewColumnTrigger = new ReshapeSignal(env, this)

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

      override def toString: String = "LazyVect{"+gen+"}"
    })


  }
}
  // NODEPLOY rename me - no longer to do with buckets (maybe something to do with stream generators?
  class ResettableBucketStreamBuild[Y, OUT](aggOut:AggOut[Y, OUT],cellReset: SliceCellLifecycle[Y], yType:ClassTag[Y], val env:Environment) {
    private def noreset() :PartialBuiltSlicedBucket[Y, OUT] = reset[Any](null, triggerAlign = AFTER)(SliceTriggerSpec.TERMINATION)

    def all() :MacroTerm[OUT] = noreset().all()
    def last() :MacroTerm[OUT] = noreset().last()

    def bind[S](stream: HasVal[S])(adder: Y => S => Unit): PartialBuiltSlicedBucket[Y, OUT] = {
      noreset().bind(stream)(adder)
    }

    def reset[S](sliceSpec:S, triggerAlign:SliceAlign = AFTER)(implicit ev:SliceTriggerSpec[S]) :PartialBuiltSlicedBucket[Y, OUT] = {
      val uncollapsed :UncollapsedGroupWithTrigger[S, _] = new UncollapsedGroupWithTrigger[S, Any](null, sliceSpec, triggerAlign, env, ev)
      new PartialBuiltSlicedBucket[Y, OUT](uncollapsed, aggOut, cellReset, env)
    }
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
