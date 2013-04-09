package typetests
import typetests.Chaining.{_}
import scala.reflect.runtime.{universe => ru}
import reflect.ClassTag
import scespet.core._
import reflect.macros.Context
import stub.gsa.esg.mekon.core.{Environment, EventGraphObject}

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 19/12/2012
 * Time: 21:22
 * To change this template use File | Settings | File Templates.
 */

class SimpleChainImpl {
  val eval: SimpleEvaluator = new SimpleEvaluator

  def query[X](stream: TraversableOnce[X]): MacroTerm[X] = {
    val eventSource = new IteratorEvents[X](stream)
    eval.addEventSource(eventSource)
    val initialTerm = new MacroTerm[X](eval)(eventSource)
    return initialTerm
  }

  def term[X](hasVal:HasVal[X]) {
    // the hasVal needs to already be a listener in this graph.
    return new MacroTerm[X](eval)(hasVal)
  }

  def run() = eval.run()
}






