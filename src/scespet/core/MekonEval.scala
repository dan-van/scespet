package scespet.core

import stub.gsa.esg.mekon.core.{EventGraphObject, Function => MFunc, Environment}

/**
 * @version $Id$
 */
class MekonEval(val env: Environment) extends FuncCollector {
  def bind(src: EventGraphObject, sink: MFunc) {
    println(s"adding listener: ${src} -> $sink")
    val out = env.addListener[Any](src.asInstanceOf[Any], sink)
  }

  def stream[X](x:X) :Expr[X] = {
    new Expr[X](new IsVal[X](x))(this)
  }
}
