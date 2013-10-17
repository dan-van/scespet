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
import org.scalatest.{OneInstancePerTest, BeforeAndAfterEach, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.{AssertionsForJUnit, ShouldMatchersForJUnit, JUnitRunner}

@RunWith(classOf[JUnitRunner])
class TestMultiTerms extends FunSuite with BeforeAndAfterEach with OneInstancePerTest with AssertionsForJUnit with ShouldMatchersForJUnit {

  var impl: SimpleEvaluator = new SimpleEvaluator()
  val postRunChecks = collection.mutable.Buffer[() => Unit]()
  override protected def beforeEach() {
    impl = new SimpleEvaluator
  }

  def addPostCheck(name:String)(check: => Unit) {
    postRunChecks.append(() => { check })
  }

  val eventsA = IteratorEvents(  0 to 5 ) ( (x,i) => (10 * i) + 1)
  val eventsB = IteratorEvents( 10 to 15 )( (x,i) => (10 * i) + 2)
  val eventsC = IteratorEvents( 20 to 25 )( (x,i) => (10 * i) + 3)

  override protected def afterEach() {
    impl.run()
    for (r <- postRunChecks) {
      r()
    }
  }

  class StreamTest[X](name:String, expected:Iterable[X], stream:Term[X]) {
    var eventI = 0
    val expectIter = expected.iterator
    stream.map(next => {
      val expect = expectIter.next()
      expectResult(expect, s"Stream $name, Event $eventI was not expected")(next)
      println(s"Observed event: $name-$eventI \t $next as expected")
      eventI += 1
    })
  }

  protected def createTestMultiStream() = {
    val set = impl.asVector(List("A", "B", "C"))
    val eventStreams = Map(
      "A" -> impl.asStream(eventsA)
      , "B" -> impl.asStream(eventsB)
      , "C" -> impl.asStream(eventsC)
    )

    set.derive(key => eventStreams(key).input)
  }

  class NamedSum[X:Numeric](val str:String) extends Reduce[X]{
    var sum = 0.0
    def add(n:X):Unit = {sum = sum + implicitly[Numeric[X]].toDouble(n)}

    override def toString = s"Sum($str)=$sum"
  }

  test("derive") {
    val multiStream = createTestMultiStream()
    new StreamTest("A", (0 to 5).toList, multiStream("A"))
    new StreamTest("B", (10 to 15).toList, multiStream("B"))
    new StreamTest("C", (20 to 25).toList, multiStream("C"))
  }

  test("reduce each") {
//    out("sums")(createTestMultiStream().reduce(new SumK[Int]("foo")).slice_pre(impl.env.getTerminationEvent))
    out("sums")(createTestMultiStream().reduce(new NamedSum[Int](_)).slice_pre(impl.env.getTerminationEvent))
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
    val countByLetter = stream.by(_.toUpper).reduce_all(new Counter)
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
}
