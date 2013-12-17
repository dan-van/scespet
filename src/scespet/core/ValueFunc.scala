package scespet.core

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:17
 * To change this template use File | Settings | File Templates.
 */
class ValueFunc[V](env:types.Env) extends UpdatingHasVal[V] {
  var value:V = _
  var newValue = false

  def setValue(newVal:V) {
    this.initialised = true
    this.value = newVal
    env.wakeupThisCycle(this)
    newValue = true
  }

  def calculate() = {
    // avoids a tricky double-fire scenario I'm investigating where a wakeupThisCycle when no one is listening gets promoted into a fireAfterChangingListeners
    if (newValue) {
      newValue = false
      true
    } else false
  }
}
