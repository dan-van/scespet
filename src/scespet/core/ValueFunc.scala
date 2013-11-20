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

  def setValue(newVal:V) {
    this.initialised = true
    this.value = newVal
    env.wakeupThisCycle(this);
  }

  def calculate() = true;
}
