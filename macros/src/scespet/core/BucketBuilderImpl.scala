package scespet.core

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:36
 * To change this template use File | Settings | File Templates.
 */
class BucketBuilderImpl[X, Y <: Reduce[X]](newBFunc:() => Y, inputTerm:MacroTerm[X], eval:FuncCollector) extends BucketBuilder[X, Y] {
  class WindowMaintainer(val windowStream: MacroTerm[Boolean]) extends HasVal[Y] with types.MFunc {
    val dataEvents = inputTerm.input
    eval.env.addListener(dataEvents, this)

    val windowEvents = windowStream.input
    eval.env.addListener(windowEvents, this)

    var inWindow = windowEvents.value

    var nextReduce : Y = _
    if (inWindow) nextReduce = newBFunc.apply()

    var completedReduce : Y = _

    def trigger = this

    def value = completedReduce

    def calculate():Boolean = {
      // add data before window close
      var fire = false

      var isNowOpen = inWindow
      if (eval.env.hasChanged(windowEvents)) {
        isNowOpen = windowEvents.value
      }
      if (isNowOpen && !inWindow) {
        // window started
        nextReduce = newBFunc.apply()
        inWindow = true
      }
      if (eval.env.hasChanged(dataEvents)) {
        // note, if the window close coincides with this event, we discard the datapoint
        // i.e. window close takes precedence
        if (isNowOpen) {
          nextReduce.add(dataEvents.value)
          // TODO: for 'fold' mode, we'd need
          // if (fireOnEvent) fire = true
        }
      }
      if (inWindow && !isNowOpen) {
        // window closed. snap the current reduction and get ready for a new one
        completedReduce = nextReduce
        nextReduce = null.asInstanceOf[Y]
        inWindow = false
        fire = true
      }
      return fire
    }
  }

  def window(windowStream: MacroTerm[Boolean]) :MacroTerm[Y] = {
    // two ways of doing this. Use Mekon primitives, or Reactive
    // actually, reactive doesn't have 'who fired' semantics, which means that we can't distinguish between window fire
    // or window and event
//    class WindowFold extends Reduce[(Y,Boolean)] {
//      var lastState:Boolean = false
//      var nextReduce:Y = _
//      def add(valAndWindow: (Y, Boolean)) = {
//        if (valAndWindow._2 && !lastState) {
//          // window started
//          nextReduce = newBFunc.apply()
//        }
//      }
//    }
//    inputTerm.join(windowStream).reduce()
//    val bucketMaintainer = new HasVal[Y] with types.MFunc
return new MacroTerm[Y](eval)(new WindowMaintainer(windowStream))
//    ???
  }

  def each(n: Int):MacroTerm[Y] = {
    val bucketTrigger = new NewBucketTriggerFactory[X, Y] {
      def create(source: HasVal[X], reduce: Y, env:types.Env) = new NthEvent(n, source.trigger, env)
    }
    return buildTermForBucketStream(newBFunc, bucketTrigger)
  }

  def buildTermForBucketStream[Y <: Reduce[X]](newBFunc:() => Y, triggerBuilder: NewBucketTriggerFactory[X, Y]):MacroTerm[Y] = {
    // todo: make Window a listenable Func
    // "start new reduce" is a pulse, which is triggered from time, input.trigger, or Y
    val input = inputTerm.input
    val listener = new BucketMaintainer[Y,X](input, newBFunc, triggerBuilder, eval.env)
    eval.bind(input.trigger, listener)
    return new MacroTerm[Y](eval)(listener)
  }
}

class NthEvent(val N:Int, val source:types.EventGraphObject, env:types.Env) extends types.MFunc {
  var n = 0;
  env.addListener(source, this)
  def calculate():Boolean = {n += 1; return n % N == 0}
}

