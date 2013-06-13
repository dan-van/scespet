package scespet.core

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 05/04/2013
 * Time: 21:29
 * To change this template use File | Settings | File Templates.
 */
class WindowedReduce[X, Y <: Reduce[X]](val dataEvents :HasValue[X], val windowEvents :HasValue[Boolean], newReduce :()=>Y, env :types.Env) extends UpdatingHasVal[Y] {
    env.addListener(dataEvents.getTrigger, this)
    env.addListener(windowEvents.getTrigger, this)

    var inWindow = windowEvents.value

    var nextReduce : Y = _
    if (inWindow) nextReduce = newReduce()

    var completedReduce : Y = _

    def value = completedReduce

  def calculate():Boolean = {
    // add data before window close
    var fire = false

    var isNowOpen = inWindow
    if (env.hasChanged(windowEvents.getTrigger)) {
      isNowOpen = windowEvents.value
    }
    if (isNowOpen && !inWindow) {
      // window started
      nextReduce = newReduce()
      inWindow = true
    }
    if (env.hasChanged(dataEvents.getTrigger)) {
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
