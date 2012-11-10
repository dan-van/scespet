package scespet.core

import gsa.esg.mekon.core.{EventGraphObject, Environment}

/**
 * @author: danvan
 * @version $Id$
 */
class MekonEval(val env: Environment) extends FuncCollector {
  def bind(src: HasVal[_], sink: Func[_, _]) {
    println(s"adding listener: ${src.trigger} -> ${sink.trigger}")
    val out = env.addListener[Any](src.trigger.asInstanceOf[Any], sink.trigger.asInstanceOf[gsa.esg.mekon.core.Function])
  }

  def stream[X](x:X) :Expr[X] = {
    new Expr[X](new IsVal[X](x))(this)
  }
}
