package programs

import collection.mutable.ArrayBuffer
import scespet.core._
import scespet.util._
import org.junit.Test


/**
* Created with IntelliJ IDEA.
* User: danvan
* Date: 21/12/2012
* Time: 09:29
* To change this template use File | Settings | File Templates.
*/
import org.scalatest.{Matchers, OneInstancePerTest, BeforeAndAfterEach, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.{AssertionsForJUnit, ShouldMatchersForJUnit, JUnitRunner}

@RunWith(classOf[JUnitRunner])
class TestSingleTerms extends FunSuite with BeforeAndAfterEach with OneInstancePerTest with AssertionsForJUnit with Matchers {
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

  var impl: SimpleEvaluator = new SimpleEvaluator()

  override protected def beforeEach() {
    impl = new SimpleEvaluator
  }

  var names = IteratorEvents(nameList)((_,_) => 0L)
  var trades = IteratorEvents(tradeList)((_,i) => i)

  //  def output(prefix:String)(term:VectTerm[_,_]) = term.collapse().map(x => println(prefix + String.valueOf(x)))

//  def output(prefix:String)(term:VectTerm[_,_]) = term.collapse().map(x => println(prefix + String.valueOf(x)))

  override protected def afterEach() {
    impl.run()
  }

  def v1 = {
    var namesExpr: MacroTerm[String] = impl.asStream(names).asInstanceOf[MacroTerm[String]]
    out("name:"){namesExpr}
  }
  def v1a = {
    var namesExpr: MacroTerm[String] = impl.asStream(names).asInstanceOf[MacroTerm[String]]
    out(".N names:"){ namesExpr.filter(_.endsWith(".N")) }
  }
  def v2 = {
    out("Vod trade bucket:") {
      var map: MacroTerm[Int] = impl.asStream(trades).filter(_.name == "VOD.L").map(_.qty)
      map.reduce(new Sum[Int]).each(2)
    }
  }
  def v3 = {
    // test multiple event sources
    var namesExpr: MacroTerm[String] = impl.asStream(names).asInstanceOf[MacroTerm[String]]
    out("Trade:") { impl.asStream(trades) }
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
//  remoteRun
//  impl.run

  class StreamTest[X](name:String, expected:Iterable[X], stream:Term[X]) {
    var eventI = 0
    val expectIter = expected.iterator
    stream.map(next => {
      val expect = expectIter.next()
      assertResult(expect, s"Stream $name, Event $eventI was not expected")(next)
      println(s"Observed event: $eventI \t $next as expected")
      eventI += 1
    })
  }

  test("stream increment") {
    val elements = 0 to 20
    val expectedOut = elements.map( _ * 1000 ).toArray

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
    val mult10 = stream.map(_ * 1000)
    new StreamTest("mult10", expectedOut, mult10)
  }

  test("reduce each") {
    val elements = 0 to 10
    val expectedOut = elements.toList.grouped(3).map( _.reduce( _+_ )).toList
    expectedOut should be(List(3, 12, 21, 19))

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
    val mult10 = stream.reduce(new Sum[Int]).each(3).map(_.sum.toInt)
    new StreamTest("mult10", expectedOut, mult10)
  }

//  @Test
  test("fold each") {
    val elements = List.fill(11)(2)
    val expectedOut = elements.grouped(3).map( _.scanLeft(0)( _+_ ).drop(1)).toList.flatten
    expectedOut should be(List(List(2, 4, 6), List(2, 4, 6), List(2, 4, 6), List(2, 4)).flatten)

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
    val mult10 = stream.fold(new Sum[Int]).each(3).map(_.sum.toInt)
    new StreamTest("fold each", expectedOut, mult10)
  }

  test("Simple reduce each") {
    out("Vod trade bucket:") {
      var qty: MacroTerm[Int] = impl.asStream(trades).filter(_.name == "VOD.L").map(_.qty)
      qty.reduce(new Sum[Int]).each(2)
    }
  }

  test("Simple fold each") {
    val vodTrades = impl.asStream(trades).filter(_.name == "VOD.L")
    out("Vod Trade")(vodTrades)
    out("Vod trade bucket:") {
      var qty: MacroTerm[Int] = vodTrades.map(_.qty)
      qty.fold(new Sum[Int]).each(2)
    }
  }
}
