package scespet

import core._
import core.types
import scespet.expression.{RootTerm, AbsTerm}

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 30/04/2013
 * Time: 23:35
 * To change this template use File | Settings | File Templates.
 */
class EnvTermBuilder(val env :types.Env) {
  def query[X <: types.MFunc](func: X) : MacroTerm[X] = {
    var hasVal = new IsVal[X](func)
    return new MacroTerm[X](env)(hasVal)
  }

  def query[X](data: HasVal[X]) : MacroTerm[X] = {
    return new MacroTerm[X](env)(data)
  }

  def query[X,Y](value: AbsTerm[X, Y]) :Term[Y] = {
    if (value.parent != null) {
      val localParent = query(value.parent)
      val localThis = value.applyTo(localParent)
      localThis
    } else if (value.isInstanceOf[ RootTerm[Y] ]) {
      var hasVal = value.asInstanceOf[RootTerm[Y]].buildHasVal(env)
      new MacroTerm[Y](env)(hasVal)
    } else {
      ???
    }
  }

}
