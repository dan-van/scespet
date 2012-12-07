import collection.mutable.ArrayBuffer
import scespet.core2.core._
import stub.gsa.esg.mekon.core.{Function => MFunc}


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


var simple = new SimpleEvaluator()

val tradeStream : Expr[Trade] = simple.events(trades)
for (t <- tradeStream;
//  t100 <- tradeStream if t100.qty == 100;
  t100 <- t;
  (v,p) <- (t.price * t.qty, t.price)
) yield {  println("last t100: "+t100+" and "+v+" ,"+p)};

simple.run()
