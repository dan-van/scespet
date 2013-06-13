package programs

import collection.mutable.ArrayBuffer
import scespet.core._
import scespet.util._

/**
* Created with IntelliJ IDEA.
* User: danvan
* Date: 21/12/2012
* Time: 09:29
* To change this template use File | Settings | File Templates.
*/
object Program1 extends App {

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

//  def output(prefix:String)(term:VectTerm[_,_]) = term.collapse().map(x => println(prefix + String.valueOf(x)))

  var names = IteratorEvents(nameList)
  var trades = IteratorEvents(tradeList)

  def v1 = {
    var namesExpr: Term[String] = impl.query(names).asInstanceOf[Term[String]]
    out("name:"){namesExpr}
  }
  def v1a = {
    var namesExpr: MacroTerm[String] = impl.query(names).asInstanceOf[MacroTerm[String]]
    out(".N names:"){ namesExpr.filter(_.endsWith(".N")) }
  }
  // todo: more single tests, macros, etc

  // now some vector tests
  def v2 = {
    var namesExpr: MacroTerm[String] = impl.query(names).asInstanceOf[MacroTerm[String]]
    out("by length:") {namesExpr.by(x => x.length)}
  }
  def v3 = {
    var namesExpr: MacroTerm[String] = impl.query(names).asInstanceOf[MacroTerm[String]]
    out("by length, mapped") {namesExpr.by(x => x.length).map(x => "prefix"+x)}
  }
  def v4 = {
    var namesExpr: MacroTerm[String] = impl.query(names).asInstanceOf[MacroTerm[String]]
    out("by length, counted") {namesExpr.by(x => x.length).fold_all_noMacro(() => {new Counter[String]})}
  }
  def v5 = {
    // test multiple event sources
    var namesExpr: MacroTerm[String] = impl.query(names).asInstanceOf[MacroTerm[String]]
    out("Trade:") { impl.query(trades) }
    out("Name:") { namesExpr }
  }
  def v6 = {
    var namesExpr: MacroTerm[String] = impl.query(names).asInstanceOf[MacroTerm[String]]
    val tradeStream = impl.query(trades)
    def getTrades(name:String) = tradeStream.filter( _.name == name)
    val nameVectTakeTrades: VectTerm[String, Trade] = namesExpr.by(x => x).mapk(k => getTrades(k).input)
    out("name to trade") {nameVectTakeTrades}
  }

//  val v3 = tradeExpr map {_.qty} reduce (new Sum, 2.samples ) map { println(_) }

  // test vector windows
  def v7 = {
    val tradeStream = impl.query(trades)
    val counter = tradeStream.fold_all(new Counter[Trade])
    val windows = counter.map(_.c % 3 != 0)
    out("WindowState: open="){windows}
    out("by name, count within window") {tradeStream.by(_.name).reduceNoMacro(new Counter[Trade]).window(windows)}
  }

  // test per-element vector windows
  def v8 = {
    val tradeStream = impl.query(trades)
    def windows(name:String) = tradeStream.filter(_.name == name).fold_all(new Counter[Trade]).map(_.c % 3 != 0).input
    out("by name, window %3 count") {tradeStream.by(_.name).reduceNoMacro(new Counter[Trade]).window(windows _)}
  }

  //  val v3 = tradeExpr map {_.qty} reduce (new Sum, 2.samples ) map { println(_) }
//  val v2 = tradeExpr by { _.name } map {_.qty} map (new Sum) map {println(_)}
  //  val v2:Term[Sum] = from(trade) map { _.name } map { _.length } reduce(new Sum, 2.hours.between("09:00", "15:00") )
  v8
  impl.run()
}
