package programs

import collection.mutable.ArrayBuffer
import scespet.core._
import typetests.SimpleChainImpl

/**
* Richer event source allowing new events at runtime, and therefore testing of mapk, join, etc
*/
object Program2 extends App {
  val names = new ArrayBuffer[String]()
  names += "MSFT.O"
  names += "VOD.L"
  names += "IBM.N"
  names += "IBM.N"
  names += "LLOY.L"
  names += "IBM.N"
  names += "BARC.L"
  val impl: SimpleChainImpl = new SimpleChainImpl()
  var namesExpr: MacroTerm[String] = impl.query(names).asInstanceOf[MacroTerm[String]]


  //  def output(prefix:String)(term:VectTerm[_,_]) = term.collapse().map(x => println(prefix + String.valueOf(x)))

  def v1 = {
    out("name:"){namesExpr}
  }
  def v2 = {
    out("by length:") {namesExpr.by(x => x.length)}
  }
  def v3 = {
    out("by length, mapped") {namesExpr.by(x => x.length).map(x => "prefix"+x)}
  }
  def v4 = {
    out("by length, counted") {namesExpr.by(x => x.length).fold_all_noMacro(() => {new Counter[String]})}
  }
//  val v3 = tradeExpr map {_.qty} reduce (new Sum, 2.samples ) map { println(_) }
//  val v2 = tradeExpr by { _.name } map {_.qty} map (new Sum) map {println(_)}
  //  val v2:Term[Sum] = from(trade) map { _.name } map { _.length } reduce(new Sum, 2.hours.between("09:00", "15:00") )
  v3
  impl.run
}
