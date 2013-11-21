package programs

import collection.mutable.ArrayBuffer
import data.Plot
import scespet.core._
import scespet.util._
import java.util.{List => JList}
import collection.JavaConversions._


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
    if (Plot.active) {
      Plot.waitForClose()
    }
  }

  class StreamTest[X](name:String, expected:Iterable[X], stream:Term[X]) {
    var eventI = 0
    val expectIter = expected.iterator
    stream.map(next => {
      if (!expectIter.hasNext) throw new AssertionError(s"$name had more events (>=${eventI+1}) than expected")
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

  test("dereference single stream from multi") {
    val multiStream = createTestMultiStream()

    new StreamTest("A", (0 to 5).toList, multiStream("A"))
    new StreamTest("B", (10 to 15).toList, multiStream("B"))
    new StreamTest("C", (20 to 25).toList, multiStream("C"))
  }

  test("subset") {
    val multiStream = createTestMultiStream()
    val subset = multiStream.subset(_ == "C")
    val singleStream = subset("C")
    new StreamTest("C", (20 to 25).toList, singleStream)
  }

  test("mapKeys") {
    val multiStream = createTestMultiStream()
    val reKeyed = multiStream.mapKeys{case k => Some(k.toLowerCase)}

    // expect no results on the old key
    new StreamTest("A", List[Int](), reKeyed("A"))

    // and all events on new keys
    new StreamTest("a", (0 to 5).toList, reKeyed("a"))
    new StreamTest("b", (10 to 15).toList, reKeyed("b"))
    new StreamTest("c", (20 to 25).toList, reKeyed("c"))
  }

  test("map subset keys") {
    val multiStream = createTestMultiStream()
    val subset = multiStream.mapKeys {
      case k if k == "C" => Some(k.toLowerCase)
      case _ => None
    }
    val keyCount = subset.mapVector(_.getSize)
    new StreamTest("keyCount", Array.fill(6)(1).toList, keyCount)

    val firstElementAsSingleStream = subset.mapVector({case v if v.getSize > 0 => v.get(0); case _ => 0})
    new StreamTest("c", (20 to 25).toList, firstElementAsSingleStream)
  }

  case class FeedEntry(feedName:String, symbol:String)

  test("join") {
    val feedData = collection.mutable.HashMap[FeedEntry, HasVal[Double]]()
    feedData += FeedEntry("Reuters", "MSFT") -> IteratorEvents(1.1 to (10.1,1))((e,i)=> i)
    feedData += FeedEntry("Reuters", "IBM")  -> IteratorEvents(20.1 to (30.0,2))((e,i)=> i*2)
    feedData += FeedEntry("CTS", "IBM")      -> IteratorEvents(20.3 to (30.0,2))((e,i)=> i*2)
    feedData += FeedEntry("UTDF", "MSFT")    -> IteratorEvents(1.2 to (10.2,1))((e,i)=> i)
    feedData += FeedEntry("Reuters", "foo")  -> IteratorEvents(1.0 to (2.0,0.1))((e,i)=> i)

    val feedDict = impl.asVector(feedData.keySet)
    val prices = feedDict.derive(key => feedData(key))

    val reuters = prices.mapKeys({case k if k.feedName == "Reuters" => Some(k.symbol); case _ => None})
    val joined = prices.join(reuters, k => k.symbol)
    val compared = joined.map(p => p._1 - p._2)
    out("diffs")(compared)
    Plot.plot(compared)
  }

  test("by") {
    val nameStream = impl.asStream(IteratorEvents(Array("FOO", "BAR", "BAZ", "FOOBAR"))( (x,i) => i.toLong + 1 ))
    val byFirst = nameStream.by(_.charAt(0))
    out("byFirst"){byFirst}
    new StreamTest("F", List("FOO", "FOOBAR"), byFirst('F'))
    new StreamTest("B", List("BAR", "BAZ"), byFirst('B'))
  }

  test("toValueSet") {
    val nameStream = impl.asStream(IteratorEvents(Array("FOO", "BAR", "BAZ", "FOOBAR"))( (x,i) => i.toLong + 1 ))
    val byFirst = nameStream.by(_.charAt(0))
    val expanded = byFirst.toValueSet(x => List(x+".1", x+".2"))
    out("byFirst "){expanded}
    impl.run(1)
    expectResult(List("FOO.1", "FOO.2"))(expanded.keys)
    expectResult(expanded.keys)(expanded.values)

    impl.run(1)
    expectResult(List("FOO.1", "FOO.2", "BAR.1", "BAR.2"))(expanded.keys)
    expectResult(expanded.keys)(expanded.values)

    impl.run(1)
    expectResult(List("FOO.1", "FOO.2", "BAR.1", "BAR.2", "BAZ.1", "BAZ.2"))(expanded.keys)
    expectResult(expanded.keys)(expanded.values)

    impl.run(1)
    expectResult(List("FOO.1", "FOO.2", "BAR.1", "BAR.2", "BAZ.1", "BAZ.2", "FOOBAR.1", "FOOBAR.2"))(expanded.keys)
    expectResult(expanded.keys)(expanded.values)
  }

}
