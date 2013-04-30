import collection.mutable.ArrayBuffer
import scespet.core._
import gsa.esg.mekon.core.{EventGraphObject, Function => MFunc}

object NowWithStateAndEval3 extends App {
/**
 * Experiments with approaches to getting Select statements
 */

case class Trade(var price:Double, var qty:Double)
var trades = new ArrayBuffer[Trade]()
trades += new Trade(1.12, 100)
trades += new Trade(2.12, 200)
trades += new Trade(3.12, 100)
trades += new Trade(4.12, 200)
trades += new Trade(5.12, 100)


class Sum extends AbsFunc[Double, Double]{
  def calculate() = {value = value + source.value; true}
}

var simple = new SimpleEvaluator()
val tradeEvents = IteratorEvents(trades)
simple.addEventSource( tradeEvents )

val tradeStream : Expr[Trade] = new Expr(tradeEvents)(simple)
//tradeStream.sel(new Select[_]{var cash = in.price * in.qty; var quantity = in.qty})
tradeStream.map(x => {println(x);10})
val turnover = tradeStream.map(x => x.price * x.qty).map(_ * 100)
turnover.map(t => println(s"Turnover $t"))


//prices.map(new Sum()).map(x => println("sum: " + x))

simple.run()



//var m = new Mekon("today")
//m.getTrades("VOD").map(_.price).map(_ * 100.0).map(x=>println("Price: " + x ))
//m.getPrices("VOD").map(_.price).map(_ * 100.0).map(x=>println("Price: " + x ))
//simple.run()
}