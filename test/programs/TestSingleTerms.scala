package programs

import collection.mutable.ArrayBuffer
import scespet.core._
import scespet.util._
import org.junit.Test
import org.scalatest.matchers.Matchers


/**
* Created with IntelliJ IDEA.
* User: danvan
* Date: 21/12/2012
* Time: 09:29
* To change this template use File | Settings | File Templates.
*/
import org.scalatest.{OneInstancePerTest, BeforeAndAfterEach, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.{AssertionsForJUnit, ShouldMatchersForJUnit, JUnitRunner}

@RunWith(classOf[JUnitRunner])
class TestSingleTerms extends FunSuite with BeforeAndAfterEach with OneInstancePerTest with AssertionsForJUnit with ShouldMatchersForJUnit {
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
  val postRunChecks = collection.mutable.Buffer[() => Unit]()
  override protected def beforeEach() {
    impl = new SimpleEvaluator
  }

  def addPostCheck(name:String)(check: => Unit) {
    postRunChecks.append(() => { check })
  }

  var names = IteratorEvents(nameList)((_,_) => 0L)
  var trades = IteratorEvents(tradeList)((_,i) => i)

  //  def output(prefix:String)(term:VectTerm[_,_]) = term.collapse().map(x => println(prefix + String.valueOf(x)))

//  def output(prefix:String)(term:VectTerm[_,_]) = term.collapse().map(x => println(prefix + String.valueOf(x)))

  override protected def afterEach() {
    impl.run()
    for (r <- postRunChecks) {
      r()
    }
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
      assert(expectIter.hasNext, s"Stream $name, Event $eventI with value $next was additional to expected")
      val expect = expectIter.next()
      expectResult(expect, s"Stream $name, Event $eventI was not expected")(next)
      println(s"Observed event: $name-$eventI \t $next as expected")
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
    val elements = (1 to 11)
    val expectedOut = elements.toList.grouped(3).map( _.reduce( _+_ )).toList
    expectedOut should be(List(6, 15, 24, 21))

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
    val mult10 = stream.reduce(new Sum[Int]).each(3).map(_.sum.toInt)
    new StreamTest("mult10", expectedOut, mult10)
  }

  test("reduce_all") {
    // fail: for this to work I need to add support for env.addListener(env.getTerminationEvent, function)
    val elements = "aaAbAB".toCharArray

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
    val count = stream.reduce_all(new Counter).map(_.c)
    new StreamTest("mult10", List(6), count)
  }

  test("fold each") {
    val elements = List.fill(11)(2)
    val expectedOut = elements.grouped(3).map( _.scanLeft(0)( _+_ ).drop(1)).toList.flatten
    expectedOut should be(List(List(2, 4, 6), List(2, 4, 6), List(2, 4, 6), List(2, 4)).flatten)

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
    val mult10 = stream.fold(new Sum[Int]).each(3).map(_.sum.toInt)
    new StreamTest("fold each", expectedOut, mult10)
  }

  test("slice before") {
    val elements = List.fill(3)(1) ::: (10 :: List.fill(3)(1))
    val expectReduce = List(3, 13)
    val expectScan = List(List(1, 2, 3), List(10, 11, 12, 13)).flatten

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
    val sliceTrigger = stream.filter(_ == 10)
    val scan = stream.fold(new Sum[Int]).slice_pre( sliceTrigger ).map(_.sum.toInt)
    val reduce = stream.reduce(new Sum[Int]).slice_pre( sliceTrigger ).map(_.sum.toInt)
    new StreamTest("scan", expectScan, scan)
    new StreamTest("reduce", expectReduce, reduce)
  }

  test("slice after") {
    val elements = List.fill(3)(1) ::: (10 :: List.fill(3)(1))
    val expectReduce = List(13, 3)
    val expectScan = List(List(1, 2, 3, 13), List(1, 2, 3)).flatten

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
    val sliceTrigger = stream.filter(_ == 10)
    val scan = stream.fold(new Sum[Int]).slice_post( sliceTrigger ).map(_.sum.toInt)
    val reduce = stream.reduce(new Sum[Int]).slice_post( sliceTrigger ).map(_.sum.toInt)
    new StreamTest("scan", expectScan, scan)
    new StreamTest("reduce", expectReduce, reduce)
  }

  test("filterType") {
    val elements = List(1, 2, 4, "Open", 8, 16, "Open", 32, 64, "Close", 128, "Close", "Open", 256, "Close", "Open", 512)
    val expectFiltered = List[Integer](1, 2, 4, 8, 16, 32, 64, 128, 256, 512)

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
// umm, don't understand this type error....
//    val intStream = stream.filterType[Integer]()
    val intStream = stream.filter(_.isInstanceOf[Integer]).map(_.asInstanceOf[Integer])
    new StreamTest("Filtered", expectFiltered, intStream)
  }

  test("reduce while") {
    val elements = List(1, 2, 4, "Open", 8, 16, "Open", 32, 64, "Close", 128, "Close", "Open", 256, "Close", "Open", 512)
    val expectReduce = List(120, 256)
//    val expectScan = List(0, 8, 24, 56, 130, 0, 256)

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
    // umm, don't understand this type error....
    //    val intStream = stream.filterType[Integer]()
    val numberStream = stream.filter(_.isInstanceOf[Integer]).map(_.asInstanceOf[Integer].intValue())
//    val numberStream = stream.filterType[Integer].map(_.intValue())
    val stringStream = stream.filterType[String]

    val isOpen = stringStream.map(_ == "Open").asInstanceOf[MacroTerm[Boolean]]

//    val scan = numberStream.fold(new Sum[Int]).window( isOpen ).map(_.sum.toInt)
    val reduce = numberStream.reduce(new Sum[Int]).window( isOpen ).map(_.sum.toInt)
//    new StreamTest("scan", expectScan, scan)
    new StreamTest("reduce", expectReduce, reduce)
  }

  test("by") {
    val elements = "aaAbAB".toCharArray
    val expectOut = Map('A' -> 4, 'B' -> 2)

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
    val byUpper = stream.by(_.toUpper)
    out("by upper")(byUpper)

    val countByLetter = byUpper.reduce_all(new Counter)
//    val countByLetter = byUpper.fold_all(new Counter)
    out("count")(countByLetter)
    addPostCheck("Counts") {
      import collection.JavaConverters._
      expectResult(expectOut.keySet, "Keyset mismatch")(countByLetter.input.getKeys.asScala.toSet)
      for (e <- expectOut) {
        var i = countByLetter.input.indexOf(e._1)
        val reduce = countByLetter.input.get(i)
        val value = reduce.c
        expectResult(e._2, s"Value mismatch for key ${e._1}, index: $i")(value)
      }
    }
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
