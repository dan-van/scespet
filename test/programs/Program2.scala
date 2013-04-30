package programs

import collection.mutable.ArrayBuffer
import scespet.core._
import scespet.util._


/**
 * test Macro implementation
*/
object Program2 extends App {

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

  def v1 = {
    val namesExpr: MacroTerm[Sum] = impl.query(trades).map(_.qty).reduce(new Sum).each(3)
    out("sum each 3 elements:"){namesExpr}
  }
  def v2 = { // now with vectors
    var namesExpr = impl.query(trades).by(_.name).map(_.qty).reduceNoMacro(new Sum).each(3)
    out("ewma each 3 elements by name:"){namesExpr}
  }
  v2
  impl.run

//  def foo
//  def fooo()

}
