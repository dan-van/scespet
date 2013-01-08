package typetests

import typetests.Chaining.{_}
import collection.mutable.ArrayBuffer

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

  class Sum extends Reduce[Int]{
    var s = 0;
    def add(i:Int):Unit = {s += i}

    override def toString = s"Sum=$s"
  }


  var tradeExpr: SimpleChainImpl[Trade] = new SimpleChainImpl(trades)
  def v1 = {
    tradeExpr map {_.qty} map (new Sum) map { println(_) }
  }
  def v2 = {
    tradeExpr map {_.qty} bucket2 (new Sum) each 2 map { println(_) }
  }
  def v2a = {
    var qtyStream = tradeExpr map {_.qty}
    qtyStream.asInstanceOf[MacroTerm[Int]].bucket2NoMacro(() => {new Sum}).each(2) map { println (_) }
  }
//  val v3 = tradeExpr map {_.qty} bucket (new Sum, 2.samples ) map { println(_) }
//  val v2 = tradeExpr by { _.name } map {_.qty} map (new Sum) map {println(_)}
  //  val v2:Term[Sum] = from(trade) map { _.name } map { _.length } bucket(new Sum, 2.hours.between("09:00", "15:00") )
  v2a
  tradeExpr.run
}
