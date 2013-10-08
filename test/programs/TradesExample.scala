package programs

import collection.mutable.ArrayBuffer
import scespet.core.{SimpleEvaluator, IteratorEvents, MacroTerm, Reduce}
import scespet.util._


/**
* Created with IntelliJ IDEA.
* User: danvan
* Date: 21/12/2012
* Time: 09:29
* To change this template use File | Settings | File Templates.
*/
class TradesExample {
  case class Trade(name:String, price:Double, qty:Int)
  var tradeList = new ArrayBuffer[Trade]()
  tradeList += new Trade("VOD", 1.12, 1)
  tradeList += new Trade("VOD", 2.12, 10)
  tradeList += new Trade("MSFT", 3.12, 2)
  tradeList += new Trade("VOD", 4.12, 100)
  tradeList += new Trade("MSFT", 5.12, 20)
  tradeList += new Trade("VOD", 6.12, 1000)
  tradeList += new Trade("MSFT", 7.12, 200)
  tradeList += new Trade("VOD", 8.12, 10000)
  tradeList += new Trade("MSFT", 9.12, 2000)

  var trades = IteratorEvents(tradeList)((_,i) => i)

  class TradePrint extends Reduce[Trade]{
    var accVol = 0
    def add(t:Trade):Unit = {accVol += t.qty; println("Reduce: "+t+" gave ACCVOL: "+accVol)}

    override def toString = s"TradeAccVol:$accVol"
  }

  val impl: SimpleEvaluator = new SimpleEvaluator()
  var tradeExpr: MacroTerm[Trade] = impl.asStream(trades).asInstanceOf[MacroTerm[Trade]]
}

object testFoldAll extends TradesExample with App {
  tradeExpr map {_.qty} fold_all (new Sum[Int]) map { println(_) }
  impl.run()
}

object testReduceEach extends TradesExample with App {
  // bucket pairs of trades into a TradePrint
  val tradeBuckets = tradeExpr.reduce(new TradePrint).each(2)
  out("tradePrint fired="){tradeBuckets}
  // bucket pairs of TradePrint into a sum (i.e. accVol of 4 trades)
  out("Sum="){ tradeBuckets.map(_.accVol).reduce(new Sum[Int]).each(2) }
  impl.run()
}


object testWindowCausal extends TradesExample with App {
  val counter: MacroTerm[Counter] = tradeExpr.fold_all(new Counter)
  // window defined to be open for the first and last 3 trades
  val windowStream = counter.map(x => (x.c <= 3 ||  (x.c >= (tradeList.size - 4) && x.c < tradeList.size)))
  out("test="){counter.join(windowStream)}  // print this window stream as it evolves

  // bucket trades into this window definition
  val tradeBuckets = tradeExpr.reduce(new TradePrint).window(windowStream)
  out("tradeBucket="){tradeBuckets}
  impl.run()
}


//  val v3 = tradeExpr map {_.qty} reduce (new Sum, 2.samples ) map { println(_) }
//  val v2 = tradeExpr by { _.name } map {_.qty} map (new Sum) map {println(_)}
//  val v2:Term[Sum] = from(trade) map { _.name } map { _.length } reduce(new Sum, 2.hours.between("09:00", "15:00") )
