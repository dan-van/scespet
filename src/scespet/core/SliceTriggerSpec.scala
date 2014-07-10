package scespet.core

import scespet.core.types.{Events, EventGraphObject}
import scala.concurrent.duration.Duration
import scespet.util.Timer

/**
 * Created by danvan on 10/07/2014.
 */
trait SliceTriggerSpec[-X] {
  // NODEPLOU - can I delete src?
  def buildTrigger(x:X, src:Set[EventGraphObject], env:types.Env) :types.EventGraphObject
}

object SliceTriggerSpec {
  val TERMINATION = new SliceTriggerSpec[Any] {
    def buildTrigger(x:Any, src: Set[EventGraphObject], env: types.Env) = {
      env.getTerminationEvent
    }
  }
  val NULL = new SliceTriggerSpec[Null] {
    def buildTrigger(x:Null, src: Set[EventGraphObject], env: types.Env) = {
      null
    }
  }

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
    override def buildTrigger(x: MacroTerm[T], src: Set[EventGraphObject], env: _root_.scespet.core.types.Env): _root_.scespet.core.types.EventGraphObject =  x.getTrigger
  }

  implicit def toTriggerSpec[T](m:MacroTerm[T]) :SliceTriggerSpec[MacroTerm[T]] = new MacroTermSliceTrigger[T](m)
}
