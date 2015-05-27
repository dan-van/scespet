package scespet.core

import scespet.core.types._
import scala.concurrent.duration.{FiniteDuration, Duration}
import scespet.util.Timer
import scespet.core.types.Events
import scespet.core.SliceTriggerSpec.EventObjIsTriggerSpec

/**
 * Created by danvan on 10/07/2014.
 */
trait SliceTriggerSpec[-X] {
  // NODEPLOU - can I delete src?
  def buildTrigger(x:X, src:Set[EventGraphObject], env:types.Env) :types.EventGraphObject

  implicit def asVectSliceSpec:VectSliceTriggerSpec[X] = {
    new VectSliceTriggerSpec[X] {
      def toTriggerSpec[K](k: K, x: X): SliceTriggerSpec[X] = {
        SliceTriggerSpec.this
      }

      override def newCellPrerequisites(x: X): Set[EventGraphObject] = Set()
    }
  }
}

object SliceTriggerSpec {
  val TERMINATION = new SliceTriggerSpec[Any] {
    def buildTrigger(x:Any, src: Set[EventGraphObject], env: types.Env) = {
      env.getTerminationEvent
    }
  }

  // NODEPLOY is this really necessary? doesn't it just make for confusion with TERMINATION. Can't we just use TERMINATION?
  /** Never fire a slice */
  val NULL = new SliceTriggerSpec[Null] {
    def buildTrigger(x:Null, src: Set[EventGraphObject], env: types.Env) = {
      null
    }
  }

//  implicit object FiniteDurationIsTriggerSpec extends SliceTriggerSpec[FiniteDuration] {
//    def buildTrigger(duration:FiniteDuration, src: Set[EventGraphObject], env: types.Env) = {
//      new Timer(duration)
//    }
//  }

  implicit object DurationIsTriggerSpec extends SliceTriggerSpec[Duration] {
    def buildTrigger(duration:Duration, src: Set[EventGraphObject], env: types.Env) = {
      new Timer(duration)
    }
  }
  implicit object EventsIsTriggerSpec extends SliceTriggerSpec[Events] {
    def buildTrigger(events:Events, src: Set[EventGraphObject], env: types.Env) = {
      new NthEvent(events.n, src, env)    }
  }
  implicit object EventObjIsTriggerSpec extends SliceTriggerSpec[EventGraphObject] {
    def buildTrigger(events:EventGraphObject, src: Set[EventGraphObject], env: types.Env) = {
      events
    }
  }
  // NODEPLOY one of these is unused
  implicit object MacroIsTriggerSpec extends SliceTriggerSpec[MacroTerm[_]] {
    def buildTrigger(events:MacroTerm[_], src: Set[EventGraphObject], env: types.Env) = {
      events.input.getTrigger
    }
  }
  // NODEPLOY one of these is unused
  implicit class MacroTermSliceTrigger[T](t:MacroTerm[T]) extends SliceTriggerSpec[MacroTerm[T]] {
    override def buildTrigger(x: MacroTerm[T], src: Set[EventGraphObject], env: types.Env): types.EventGraphObject =  x.getTrigger
  }

  implicit def toTriggerSpec[T](m:MacroTerm[T]) :SliceTriggerSpec[MacroTerm[T]] = new MacroTermSliceTrigger[T](m)
}

trait VectSliceTriggerSpec[-X] {
  def toTriggerSpec[K](k: K, x: X): SliceTriggerSpec[X]
  def newCellPrerequisites(x:X) :Set[EventGraphObject]
}

object VectSliceTriggerSpec {
  implicit object VectIsTriggerSpec extends VectSliceTriggerSpec[VectTerm[_, _]] {

    def toTriggerSpec[K](k: K, x: VectTerm[_, _]): SliceTriggerSpec[VectTerm[_, _]] = {
      new SliceTriggerSpec[VectTerm[_,_]] {
        override def buildTrigger(x: VectTerm[_, _], src: Set[EventGraphObject], env: Env): EventGraphObject = {
          val vector = x.input
          val i = vector.asInstanceOf[VectorStream[K,_]].indexOf(k)
          if (i < 0) {
            println(k+" is not a key in the given triggerSpec vector")
            throw new RuntimeException("when does this happen?")
            val cell = x.asInstanceOf[VectTerm[K,_]].apply(k)
            cell.trigger
          } else {
            val trigger = vector.getTrigger(i)
            trigger
          }
        }
      }
    }

    override def newCellPrerequisites(x: VectTerm[_, _]): Set[EventGraphObject] = {
      // we want to ensure that the the state of the slice-trigger vector is precomputed before a new cell needs it
      Set(x.input.getNewColumnTrigger)
    }
  }

  // NODEPLOY - either this one of the SliceTriggerSpec.toVectSliceTrigger is redundant. Which is it?
  implicit def sliceSpecToVectSliceSpec[X](implicit ev:SliceTriggerSpec[X]):VectSliceTriggerSpec[X] = {
    new VectSliceTriggerSpec[X] {
      def toTriggerSpec[K](k: K, x: X): SliceTriggerSpec[X] = {
        ev
      }

      override def newCellPrerequisites(x: X): Set[EventGraphObject] = Set()
    }
  }
}
