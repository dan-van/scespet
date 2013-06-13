package scespet.core

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:17
 * To change this template use File | Settings | File Templates.
 */
class ValueFunc[V](var value:V, env:types.Env) extends UpdatingHasVal[V] {
  def setValue(newVal:V) {
    this.value = newVal
    env.wakeupThisCycle(this);
  }

  def calculate() = true;
}
