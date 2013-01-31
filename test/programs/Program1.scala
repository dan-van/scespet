package programs

import collection.mutable.ArrayBuffer
import scespet.core.{MacroTerm, Reduce}
import typetests.{SimpleChainImpl}

/**
* Created with IntelliJ IDEA.
* User: danvan
* Date: 21/12/2012
* Time: 09:29
* To change this template use File | Settings | File Templates.
*/
object Program1 extends App {
  case class Trade(name:String, price:Double, qty:Int)
  var trades = new ArrayBuffer[Trade]()
  trades += new Trade("VOD", 1.12, 1)
  trades += new Trade("VOD", 2.12, 10)
  trades += new Trade("MSFT", 3.12, 2)
  trades += new Trade("VOD", 4.12, 100)
  trades += new Trade("MSFT", 5.12, 20)
  trades += new Trade("VOD", 6.12, 1000)
  trades += new Trade("MSFT", 7.12, 200)
  trades += new Trade("VOD", 8.12, 10000)
  trades += new Trade("MSFT", 9.12, 2000)

  class Sum extends Reduce[Int]{
    var s = 0;
    def add(i:Int):Unit = {println(s"Adding $i to sum:$s = ${s+i}");s += i}

    override def toString = s"Sum=$s"
  }

  class TradePrint extends Reduce[Trade]{
    var accVol = 0
    def add(t:Trade):Unit = {accVol += t.qty; println("Reduce: "+t+" gave ACCVOL: "+accVol)}
  }


  val impl: SimpleChainImpl = new SimpleChainImpl()
  var tradeExpr: MacroTerm[Trade] = impl.query(trades).asInstanceOf[MacroTerm[Trade]]

  def output(prefix:String)(term:MacroTerm[_]) = term.map(x => println(prefix + String.valueOf(x)))

  def v1 = {
    tradeExpr map {_.qty} map (new Sum) map { println(_) }
  }
  def v2 = {
//    tradeExpr map {_.qty} bucket2 (new Sum) each 2 map { println(_) }
//    println(tradeExpr.initialTerm.bucketFoo(new TradePrint).each(2))
    val tradeBuckets = tradeExpr.bucket(new TradePrint).each(2)
    output("tradePrint fired="){tradeBuckets}
    output("Sum="){ tradeBuckets.map(_.accVol).bucket(new Sum).each(2) }
  }
  def v2a = {
    var qtyStream = tradeExpr map {_.qty}
    qtyStream.bucket2NoMacro(() => {new Sum}).each(2) map { println (_) }
  }
//  val v3 = tradeExpr map {_.qty} bucket (new Sum, 2.samples ) map { println(_) }
//  val v2 = tradeExpr by { _.name } map {_.qty} map (new Sum) map {println(_)}
  //  val v2:Term[Sum] = from(trade) map { _.name } map { _.length } bucket(new Sum, 2.hours.between("09:00", "15:00") )
  v2
  impl.run
}
