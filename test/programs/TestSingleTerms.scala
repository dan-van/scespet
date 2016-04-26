package programs

import collection.mutable.ArrayBuffer
import scespet.core._
import scespet.util._
import org.junit.Test
import scespet.core.types.MFunc
import scespet.EnvTermBuilder
import scala.concurrent.duration._
import scespet.core.types._


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
class TestSingleTerms extends ScespetTestBase with BeforeAndAfterEach with OneInstancePerTest with AssertionsForJUnit with ShouldMatchersForJUnit {
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

  var env:SimpleEnv = _
  var impl:EnvTermBuilder = _

  override protected def beforeEach() {
    env = new SimpleEnv
    impl = EnvTermBuilder(env)
  }

  var names = IteratorEvents(nameList)((_,_) => 0L)
  var trades = IteratorEvents(tradeList)((_,i) => i)

  //  def output(prefix:String)(term:VectTerm[_,_]) = term.collapse().map(x => println(prefix + String.valueOf(x)))

//  def output(prefix:String)(term:VectTerm[_,_]) = term.collapse().map(x => println(prefix + String.valueOf(x)))

  override protected def afterEach() {
    env.run()
    super.afterEach()
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
      var map = impl.asStream(trades).filter(_.name == "VOD.L").map(_.qty)
      map.group(2.events).reduce(new Sum[Int])
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
    val mult10 = stream.group(3.events).reduce(new Sum[Int]).map(_.toInt)
    new StreamTest("mult10", expectedOut, mult10)
  }

  test("reduce_all") {
    // fail: for this to work I need to add support for env.addListener(env.getTerminationEvent, function)
    val elements = "aaAbAB".toCharArray

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
    val count = stream.reduce_all(new Counter)
    new StreamTest("mult10", List(6), count)
  }

  test("fold each") {
    val elements = List.fill(11)(2)
    val expectedOut = elements.grouped(3).map( _.scanLeft(0)( _+_ ).drop(1)).toList.flatten
//    val expectedOut = elements.grouped(3).map( _.scanLeft(0)( _+_ )).toList.flatten
    expectedOut should be(List(List(2, 4, 6), List(2, 4, 6), List(2, 4, 6), List(2, 4)).flatten)

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )

    // need to express the following combinations:
    // scan, reset every X
    // lastValue, reset every X
    // scan, all
    // lastValue
    // todo: add support for a property to control exposing empty buckets
    val mult10 = stream.group(3.events).scan(new Sum[Int]).map(_.toInt)

//    val mult10 = stream.agg(new Sum[Int]).every(3.events, reset = BEFORE)
//    val mult10 = stream.agg(new Sum[Int]).every(3.minutes)
//    val mult10 = stream.reduce(new Sum[Int]).every(3.minutes)
//    val mult10 = stream.reduce(new Sum[Int])(3.events).lastVal
//    val mult10 = stream.agg(new Sum[Int]).last.after(3.events)
//    val mult10 = stream.agg(new Sum[Int]).before(3.minutes)
//    val mult10 = stream.agg(new Sum[Int]).within(window)
//    val mult10 = stream.agg(3.events)(new Sum[Int]).last
//    val mult10 = stream.agg(window)(new Sum[Int]).last
    new StreamTest("fold each", expectedOut, mult10)
  }

  test("slice before") {
    val elements = List.fill(3)(1) ::: (10 :: List.fill(3)(1))
    val expectReduce = List(3, 13).map(_.toDouble)
    val expectScan = List(List(1, 2, 3), List(10, 11, 12, 13)).flatten.map(_.toDouble)

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
    val sliceTrigger = stream.filter(_ == 10)
    val scan = stream.group( sliceTrigger, SliceAlign.BEFORE).scan(new Sum[Int])
//    val reduce = stream.group( sliceTrigger, SliceAlign.BEFORE).reduce(new Sum[Int])
    new StreamTest("scan", expectScan, scan)
//    new StreamTest("reduce", expectReduce, reduce)
  }

  test("slice after") {
    val elements = List.fill(3)(1) ::: (10 :: List.fill(3)(1))
    val expectReduce = List(13, 3).map(_.toDouble)
    val expectScan = List(List(1, 2, 3, 13), List(1, 2, 3)).flatten.map(_.toDouble)

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
    val sliceTrigger = stream.filter(_ == 10)
    val scan = stream.group( sliceTrigger, SliceAlign.AFTER).scan(new Sum[Int])
    val reduce = stream.group( sliceTrigger, SliceAlign.AFTER).reduce(new Sum[Int])
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
    val expectReduce = List(120, 256).map(_.toDouble)
//    val expectScan = List(0, 8, 24, 56, 130, 0, 256)

    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
    // umm, don't understand this type error....
    //    val intStream = stream.filterType[Integer]()
    val numberStream = stream.filter(_.isInstanceOf[Integer]).map(_.asInstanceOf[Integer].intValue())
//    val numberStream = stream.filterType[Integer].map(_.intValue())
    val stringStream = stream.filterType[String]

    val isOpen = stringStream.map(_ == "Open").asInstanceOf[MacroTerm[Boolean]]

//    val scan = numberStream.fold(new Sum[Int]).window( isOpen ).map(_.toInt)
    val reduce = numberStream.window(isOpen).reduce(new Sum[Int])
//    new StreamTest("scan", expectScan, scan)
    new StreamTest("reduce", expectReduce, reduce)
  }

  //NODEPLOY - enable this
//  test("by") {
//    val elements = "aaAbAB".toCharArray
//    val expectOut = Map('A' -> 4, 'B' -> 2)
//
//    val stream = impl.asStream( IteratorEvents(elements)((_,_) => 0L) )
//    val byUpper = stream.by(_.toUpper)
//    out("by upper")(byUpper)
//
//    val countByLetter = byUpper.reduce(new Counter)
////    val countByLetter = byUpper.fold_all(new Counter)
//    out("count")(countByLetter)
//    addPostCheck("Counts") {
//      import collection.JavaConverters._
//      expectResult(expectOut.keySet, "Keyset mismatch")(countByLetter.input.getKeys.asScala.toSet)
//      for (e <- expectOut) {
//        var i = countByLetter.input.indexOf(e._1)
//        val value = countByLetter.input.get(i)
//        expectResult(e._2, s"Value mismatch for key ${e._1}, index: $i")(value)
//      }
//    }
//  }

  test("Simple reduce each") {
    out("Vod trade bucket:") {
      var qty: MacroTerm[Int] = impl.asStream(trades).filter(_.name == "VOD.L").map(_.qty)
      qty.group(2.events).reduce(new Sum[Int])
    }
  }

  test("Simple fold each") {
    val vodTrades = impl.asStream(trades).filter(_.name == "VOD.L")
    out("Vod Trade")(vodTrades)
    out("Vod trade bucket:") {
      var qty: MacroTerm[Int] = vodTrades.map(_.qty)
      qty.group(2.events).scan(new Sum[Int])
    }
  }


  class MyBucket extends MFunc {
    var countA = 0
    var countB = 0

    def calculate(): Boolean = {
      true
    }

    def addA(a:Int) {countA += 1}
  }
  test("Bucket join") {
    val elementsA = List(1, 2, 3, 4)
    val elementsB = List(10, 30)
    val streamA = impl.asStream( IteratorEvents(elementsA)((x,_) => x.toLong) )
    val streamB = impl.asStream( IteratorEvents(elementsB)((x,_) => (x/10).toLong) )

    val universe = impl.asVector(List("A"))
//    universe.reduceB(new MyBucket).join(streamA){}
  }

  class MySum {
    var myInt = 0
    def setMyInt(i:Int) {
      myInt = i
    }
  }

  object Base {
    def reduce[X](x:X) :Reducer[X] = new Reducer(x)
  }
  class Reducer[X](var x:X) {
    var intAdderFunc:(X,Int) => Unit = _
    def setIntAdder(func:(X,Int) => Unit) = {
      intAdderFunc = func
      this
    }
    def setIntAdder2(func:X => Int => Unit) = {
      intAdderFunc = Function.uncurried(func)
      this
    }
    def addInt(i:Int) {
      intAdderFunc(x, i)
    }
  }
  var r1 = Base.reduce(new MySum()).setIntAdder((s,i) => s.setMyInt(i))
  r1.addInt(10)
  var r2 = Base.reduce(new MySum()).setIntAdder(_.setMyInt(_))
  var r3 = Base.reduce(new MySum()).setIntAdder2(_.setMyInt)
  r2.addInt(10)
  Base.reduce(new MySum()).setIntAdder2(b => {i => b.setMyInt(i + 10)})
  Base.reduce(new MySum()).setIntAdder2(b => {i => b.myInt = i})

}
