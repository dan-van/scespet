package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.core.types.MFunc


class NthEvent(val N:Int, val sources:Set[types.EventGraphObject], env:types.Env) extends types.MFunc {
  var n = 0
  sources.foreach(env.addListener(_, NthEvent.this))
  def calculate():Boolean = {n += 1; return n % N == 0}
}

