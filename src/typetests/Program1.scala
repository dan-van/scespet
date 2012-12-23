package typetests

import typetests.Chaining.{VectTerm, Term, Reduce}
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
  trades += new Trade("VOD", 1.12, 100)
  trades += new Trade("VOD", 2.12, 200)
  trades += new Trade("MSFT", 3.12, 100)
  trades += new Trade("VOD", 4.12, 200)
  trades += new Trade("MSFT", 5.12, 200)

  class Sum extends Reduce[Int]{
    var s = 0;
    def add(i:Int):Unit = {s += i}

    override def toString = s"Sum=$s"
  }


  var tradeExpr: SimpleChainImpl[Trade] = new SimpleChainImpl(trades)
  val v1 = tradeExpr map {_.name} map {_.length} map (new Sum) map { println(_) }
//  val v2:VectTerm[_,_] = tradeExpr by { _.name } map (new Sum) map {println(_)}
  //  val v2:Term[Sum] = from(trade) map { _.name } map { _.length } bucket(new Sum, 2.hours.between("09:00", "15:00") )

  tradeExpr.run
}
