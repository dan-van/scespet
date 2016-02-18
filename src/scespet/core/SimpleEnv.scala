package scespet.core

import scespet.core.types.MFunc

import collection.mutable
import gsa.esg.mekon.core._
import java.util.TimeZone
import gsa.esg.mekon.core.EventGraphObject.Lifecycle
import java.lang.Iterable
import java.util.concurrent.TimeUnit

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 30/04/2013
 * Time: 22:54
 * To change this template use File | Settings | File Templates.
 */
class SimpleEnv() extends Environment {
  var eventI = 0
  var eventTime :Long = _

  val graph = new SlowGraphWalk

  val eventSources = mutable.Set[EventSource]()
  val initialised = mutable.Set[EventSource]()

  val terminationEvent = new types.MFunc {
    def calculate(): Boolean = {
      println("Termination event firing")
      true
    }
  }

  val eventSourceQueue = {
    var ordering = new Ordering[EventSource] {
      def compare(x: EventSource, y: EventSource): Int = (y.getNextTime - x.getNextTime).toInt
    }
    mutable.PriorityQueue[EventSource]()(ordering)
  }

  def registerEventSource(events: EventSource) {
    if (eventSources.add(events)) {
      if (events.hasNext()) {
        eventSourceQueue.enqueue( events )
      } else {
        println("Empty event source added")
      }
    }
  }


  def setStickyInGraph(source: EventGraphObject, sticky: Boolean) {
    if (sticky && source.isInstanceOf[EventSource]) {
      registerEventSource(source.asInstanceOf[EventSource])
    }
  }

  def run() {run(1000)}

  def run(n:Int) {
    if (eventI == 0) {
      if (!eventSourceQueue.isEmpty) eventTime = eventSourceQueue.head.getNextTime
    }
    graph.applyChanges()
    if (initialised.size < eventSources.size) {
      val initTime = if (eventTime != 0) {
        eventTime
      } else {
        // this is a duff idea - I think I should just have an option whether eventTime represents a counter, or timestamp
        val nonZero = for (e <- eventSources if (e.hasNext && e.getNextTime != 0)) yield e.getNextTime
        if (nonZero.isEmpty) 0 else nonZero.min
      }
      for (e <- eventSources if !initialised.contains(e)) {
        e.init(initTime, TimeUnit.DAYS.toMillis(1000 * 365))
        initialised.add( e )
      }
    }
    graph.applyChanges()

    val stopAt = eventI + n
    while (! eventSourceQueue.isEmpty && eventI < stopAt) {
      eventI += 1
      val nextSource = eventSourceQueue.dequeue()
//      println(s"\nFiring event $eventI from $nextSource, hasNext= ${nextSource.hasNext()}");
      eventTime = nextSource.getNextTime
      nextSource.advanceState()
      graph.fire(nextSource)
      if (nextSource.hasNext()) {
        eventSourceQueue.enqueue( nextSource )
      } else {
        println(s"terminated ${nextSource}")
      }
    }
    graph.fire(terminationEvent)
    import collection.JavaConverters._
    if (eventSourceQueue.isEmpty) {
      for (n <- graph.getAllNodes.asScala) {
        val graphObject = n.getGraphObject
        if (graphObject.isInstanceOf[Lifecycle]) {
          graphObject.asInstanceOf[Lifecycle].destroy()
        }
      }
    }
  }

  def wakeupThisCycle(target: types.MFunc) {
    graph.wakeup(target)
  }

  def executeDelayed(runnable: Runnable) = ???

  @Override
  def getEventTime = eventTime


  override def getRootTriggers = ???

  def getTerminationEvent: EventGraphObject = terminationEvent

  def addListener[T](source: Any, sink: types.EventGraphObject) {
    if (source.isInstanceOf[EventSource]) {
      registerEventSource(source.asInstanceOf[EventSource])
    }

    graph.addTrigger(source.asInstanceOf[types.EventGraphObject], sink.asInstanceOf[types.MFunc])
  }


  override def removeListener(source: EventGraphObject, sink: Function) = {
    graph.removeTrigger(source.asInstanceOf[types.EventGraphObject], sink.asInstanceOf[types.MFunc])
  }


  override def addWakeupReceiver[T <: EventGraphObject](source: T, wakeupTarget: Function) :T = {
    graph.addWakeupDependency(source.asInstanceOf[types.EventGraphObject], wakeupTarget.asInstanceOf[types.MFunc])
    source
  }

  override def removeWakeupReceiver(source: EventGraphObject, wakeupTarget: Function) = {
    graph.removeWakeupDependency(source.asInstanceOf[types.EventGraphObject], wakeupTarget.asInstanceOf[types.MFunc])
  }

  def hasChanged(trigger: Any):Boolean = {
    graph.hasChanged(trigger.asInstanceOf[EventGraphObject])
  }

  def hasChanged(trigger: EventGraphObject) :Boolean = graph.hasChanged(trigger)

  def isInitialised(trigger: EventGraphObject) = graph.isInitialised(trigger)

  def isFiring(trigger: EventGraphObject) = graph.isFiring(trigger)

  def getTriggers(function: scala.Any): Iterable[EventGraphObject] = getTriggers(function.asInstanceOf[MFunc])

  def getTriggers(function: Function): Iterable[EventGraphObject] = {
    graph.getTriggers(function.asInstanceOf[EventGraphObject])
  }

  def getSharedObject[T](clazz: Class[T], args : AnyRef* ) = ???

  def getSystemId = ???

  def getSystemTimezone = ???

  def getClockDate(tz: TimeZone) = ???

  def addListener[T](provider: T, consumer: Function) :T = {
    addListener(provider.asInstanceOf[Any], consumer.asInstanceOf[types.EventGraphObject]);
    provider
  }

  def removeListener(provider: Any, consumer: Function) {}

  def addOrdering[T <: EventGraphObject](provider: T, consumer: EventGraphObject) = ???

  def removeOrdering(provider: EventGraphObject, consumer: EventGraphObject) {}

  def registerService(service: Service) {}

  def getService[T <: Service](serviceClass: Class[T]) = ???

  def getSharedObject[T](clazz: Class[T], constructorSig: Array[Class[_]], args: AnyRef*) = ???

  def getClockTime = ???

  def getStartTime = ???

  def getEndTime = ???

  def isRealtime = ???

  def isShuttingDown = ???

  def isCurrentThreadWithinFire = ???

  def prettyPrintClockTime() = ???

  def prettyPrintTime(t: Long) = ???

  def invokeAtRealtime(task: Runnable, wakeupAfterRunnable: Function) {}

  def getProperty(propertyName: String) = ???

  def getProperty(propertyName: String, defaultValue: String) = ???

  def getProperty(propertyName: String, defaultValue: Double) = ???

  def getProperty(propertyName: String, defaultValue: Int) = ???

  def getProperty(propertyName: String, defaultValue: Long) = ???

  def getProperty(propertyName: String, defaultValue: Boolean) = ???

  def getApplicationProperties = ???

  def substitute(valueString: String) = ???

  def wakeupThisCycle(graphObject: EventGraphObject) {
    graph.wakeup(graphObject)
  }

  def fireAfterChangingListeners(function: Function) {
    graph.fireAfterChangingListeners(function)
  }

  def getDelayedExecutor(wakeupTarget: Function) = ???

  def shutDown(reason: String, error: Throwable) {}

  def getRootEnvironment = ???
}

