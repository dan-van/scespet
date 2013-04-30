package programs

import collection.mutable.ArrayBuffer
import scespet.core._
import scespet.TermBuilder
import scespet.util._


/**
* Created with IntelliJ IDEA.
* User: danvan
* Date: 21/12/2012
* Time: 09:29
* To change this template use File | Settings | File Templates.
*/
object TestSingleTerms extends App {

  case class Trade(name:String, price:Double, qty:Int)
  var tradeList = new ArrayBuffer[Trade]()
  tradeList += new Trade("VOD.L", 1.12, 1)
  tradeList += new Trade("VOD.L", 2.12, 10)
  tradeList += new Trade("MSFT.O", 3.12, 2)
  tradeList += new Trade("VOD.L", 4.12, 100)
  tradeList += new Trade("MSFT.O", 5.12, 20)
  tradeList += new Trade("VOD.L", 6.12, 1000)
  tradeList += new Trade("MSFT.O", 7.12, 200)
  tradeList += new Trade("VOD.L", 8.12, 10000)
  tradeList += new Trade("MSFT.O", 9.12, 2000)

  val nameList = new ArrayBuffer[String]()
  nameList += "MSFT.O"
  nameList += "VOD.L"
  nameList += "IBM.N"
  nameList += "IBM.N"
  nameList += "LLOY.L"
  nameList += "IBM.N"
  nameList += "BARC.L"

  val impl: SimpleEvaluator = new SimpleEvaluator()
  var names = IteratorEvents(nameList)
  var trades = IteratorEvents(tradeList)//  def output(prefix:String)(term:VectTerm[_,_]) = term.collapse().map(x => println(prefix + String.valueOf(x)))

//  def output(prefix:String)(term:VectTerm[_,_]) = term.collapse().map(x => println(prefix + String.valueOf(x)))

  def v1 = {
    var namesExpr: MacroTerm[String] = impl.query(names).asInstanceOf[MacroTerm[String]]
    out("name:"){namesExpr}
  }
  def v1a = {
    var namesExpr: MacroTerm[String] = impl.query(names).asInstanceOf[MacroTerm[String]]
    out(".N names:"){ namesExpr.filter(_.endsWith(".N")) }
  }
  def v2 = {
    out("Vod trade bucket:") {
      impl.query(trades).filter(_.name == "VOD.L").map(_.qty).reduce(new Sum).each(2)
    }
  }
  def v3 = {
    // test multiple event sources
    var namesExpr: MacroTerm[String] = impl.query(names).asInstanceOf[MacroTerm[String]]
    out("Trade:") { impl.query(trades) }
    out("Name:") { namesExpr }
  }

  def remoteRun = {
//    var myProgrm = out(".N names:"){ new TermBuilder().query(names).filter(_.endsWith(".N")) }.eval.asInstanceOf[TermBuilder]
//
//    val newProg = new SimpleEvaluator()
//    myProgrm.copyInto(newProg)
//
//    newProg.run()
  }
  //  val v3 = tradeExpr map {_.qty} reduce (new Sum, 2.samples ) map { println(_) }
//  val v2 = tradeExpr by { _.name } map {_.qty} map (new Sum) map {println(_)}
  //  val v2:Term[Sum] = from(trade) map { _.name } map { _.length } reduce(new Sum, 2.hours.between("09:00", "15:00") )
  remoteRun
//  impl.run
}
