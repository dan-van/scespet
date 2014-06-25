package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.core.types.MFunc


class NthEvent(val N:Int, val sources:Set[types.EventGraphObject], env:types.Env) extends types.MFunc {
  var n = 0
  sources.foreach(in => {
    env.addListener(in, NthEvent.this)
    if (env.hasChanged(in)) {
      n = 1 // initialise if the current input is already firing
    }
  })
  def calculate():Boolean = {n += 1; return n % N == 0}
}

