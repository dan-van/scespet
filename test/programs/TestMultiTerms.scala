package programs

import collection.mutable.ArrayBuffer
import data.Plot
import scespet.core._
import scespet.util._
import java.util.{List => JList}
import collection.JavaConversions._
import scala.collection.mutable
import scespet.EnvTermBuilder


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

  var env:SimpleEnv = _
  var impl:EnvTermBuilder = _

  val postRunChecks = collection.mutable.Buffer[() => Unit]()
  override protected def beforeEach() {
    env = new SimpleEnv
    impl = EnvTermBuilder(env)
  }

  def addPostCheck(name:String)(check: => Unit) {
    postRunChecks.append(() => { check })
  }

  val eventsA = IteratorEvents(  0 to 5 ) ( (x,i) => (10 * i) + 1)
  val eventsB = IteratorEvents( 10 to 15 )( (x,i) => (10 * i) + 2)
  val eventsC = IteratorEvents( 20 to 25 )( (x,i) => (10 * i) + 3)

  override protected def afterEach() {
    env.run()
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
    env.run(1)
    expectResult(List("FOO.1", "FOO.2"))(expanded.keys)
    expectResult(expanded.keys)(expanded.values)

    env.run(1)
    expectResult(List("FOO.1", "FOO.2", "BAR.1", "BAR.2"))(expanded.keys)
    expectResult(expanded.keys)(expanded.values)

    env.run(1)
    expectResult(List("FOO.1", "FOO.2", "BAR.1", "BAR.2", "BAZ.1", "BAZ.2"))(expanded.keys)
    expectResult(expanded.keys)(expanded.values)

    env.run(1)
    expectResult(List("FOO.1", "FOO.2", "BAR.1", "BAR.2", "BAZ.1", "BAZ.2", "FOOBAR.1", "FOOBAR.2"))(expanded.keys)
    expectResult(expanded.keys)(expanded.values)
  }

  // an 'MFunc' which tracks how many x, y, or both events have occured
  class XYCollector() extends Bucket with Cloneable {
    def value = this
    var firstX:Int = -1
    var lastX:Int = -1
    var xChanged,yChanged = false
    var countX, countY, countBoth = 0
    var done = false

    override def complete() {
      if (done) throw new AssertionError("double call to done: "+this)
      done = true
    }

    def calculate(): Boolean = {
      if (xChanged && yChanged) countBoth += 1
      else if (xChanged) countX += 1
      else if (yChanged) countY += 1
      else if (!done) fail("calculate called when not complete, and nothing had changed")
      xChanged = false; yChanged = false
      true
    }
    def addX(x:Int) = {xChanged = true; lastX = x; if (firstX == -1) firstX = x}
    def addY(x:Int) = yChanged = true

    override def toString: String = s"firstX:$firstX x:$lastX nx:$countX, ny:$countY, nboth:$countBoth, done:$done"

    override def equals(o: scala.Any): Boolean = {
      o match {
        case xy:XYCollector => {
          xy.countX == this.countX &&
          xy.countY == this.countY &&
          xy.countBoth == this.countBoth &&
          xy.firstX == this.firstX &&
          xy.lastX == this.lastX
        }
        case _ => false
      }
    }

    override def hashCode(): Int = {
      var result: Int = firstX
      result = 31 * result + lastX
      result = 31 * result + countX
      result = 31 * result + countY
      result = 31 * result + countBoth
      result = 31 * result + (if (done) 1 else 0)
      return result
    }

    override def clone(): XYCollector = super.clone().asInstanceOf[XYCollector]
  }

  test("bucket sliced reduce") {
    val stream = byOddEvenSliceEleven(reduce = true, 26)
    val recombinedEventCount = stream.mapVector(_.getValues.foldLeft[Int]( 0 ) ((c, bucket) => c + bucket.countBoth + bucket.countX + bucket.countY))
    new StreamTest("Recombined count", List(11, 11, 5), recombinedEventCount)


    val evenBuckets = stream("Even")
    val expectedEven = List(
                        {val v = new XYCollector; v.firstX =  0; v.lastX = 10; v.countX = 4; v.countBoth = 2;v.done=true; v}, // for i 0 -> 10, there are 3 even numbers and 2 (even & multiple of 5)
                        {val v = new XYCollector; v.firstX = 12; v.lastX = 20; v.countX = 4; v.countBoth = 1;v.done=true; v}, // for i 12 -> 21, there are 3 even numbers and 2 (even & multiple of 5)
                        {val v = new XYCollector; v.firstX = 22; v.lastX = 26; v.countX = 3; v.countBoth = 0;v.done=true; v}  // for i 22 -> 26, there are 3 even numbers and 2 (even & multiple of 5)
                      )
    new StreamTest("Even.x", expectedEven, evenBuckets)

    val oddBuckets = stream("Odd")
    val expectedOdd = List(
                        {val v = new XYCollector; v.firstX =  1; v.lastX =  9; v.countX = 4; v.countBoth = 1; v.done=true; v}, // for i 0 -> 10, there are 3 even numbers and 2 (even & multiple of 5)
                        {val v = new XYCollector; v.firstX = 11; v.lastX = 21; v.countX = 5; v.countBoth = 1; v.done=true; v}, // for i 11 -> 21, there are 3 even numbers and 2 (even & multiple of 5)
                        {val v = new XYCollector; v.firstX = 23; v.lastX = 25; v.countX = 1; v.countBoth = 1; v.done=true; v}  // for i 22 -> 26, there are 3 even numbers and 2 (even & multiple of 5)
                      )
    new StreamTest("Odd", expectedOdd, oddBuckets)
  }
  
  test("bucket sliced fold") {
    val stream = byOddEvenSliceEleven(reduce = false, 26)
    val recombinedCounter = stream.mapVector(v => {val V = v.getValues.map(_.lastX); if (V.isEmpty) 0 else V.max})
    new StreamTest("Recombined count", (0 to 26), recombinedCounter)

    // now populate an expected sequence of observed values:
    val expectedEven = new ArrayBuffer[XYCollector]()
    val expectedOdd = new ArrayBuffer[XYCollector]()
    def fillExpectations(i:Int, j:Int) {
      val vOdd = new XYCollector;
      val vEven = new XYCollector;
      for (x <- i to j) {
        // mutate the last odd or even state accumulation
        val v = if (x % 2 == 0) vEven else vOdd
        if (v.firstX < 0) v.firstX = x
        v.lastX = x
        if (x % 5 == 0) {
          v.countBoth += 1
        } else {
          v.countX += 1
        }
        // now clone and put the copy into expected
        (if (x % 2 == 0) expectedEven else expectedOdd).append(v.clone)
      }
      expectedEven.last.done = true
      expectedOdd.last.done = true
    }
    fillExpectations(0, 10)
    fillExpectations(11, 21)
    fillExpectations(22, 26)

    new StreamTest("Even", expectedEven, stream("Even"))
    new StreamTest("Odd", expectedOdd, stream("Odd"))
  }

  def byOddEvenSliceEleven(reduce :Boolean, n:Int) = {
    // set up two vector streams that fire events, sometimes co-inciding, sometimes independent
    val counter = impl.asStream(IteratorEvents(0 to n)((x,i)=>i.toLong))
    val evenOdd = counter.by(c => if (c % 2 == 0) "Even" else  "Odd")
    val div5 = evenOdd.filter(_ % 5 == 0)

    def mySliceDef(i:Int) = {
      val doIt = i > 0 && i % 11 == 0
      if (doIt)
        println("Slice!")
      doIt
    }
    val sliceOn = counter.filter(mySliceDef)

    val slicer = evenOdd.deriveSliced(k => new XYCollector)
    val sliceBuilder = if (reduce) slicer.reduce() else slicer.fold()
    val buckets = sliceBuilder.
      join(evenOdd){b => b.addX}.
      join(div5){b => b.addY}.
      slice_pre( sliceOn )
    buckets
  }

  // make this cover both fold and reduce
  test("bucket windows") {
    // set up two vector streams that fire events, sometimes co-inciding, sometimes independent
    val counter = impl.asStream(IteratorEvents(0 to 26)((x,i)=>i.toLong))
    val evenOdd = counter.by(c => if (c % 2 == 0) "Even" else  "Odd")
    val div5 = evenOdd.filter(_ % 5 == 0)

    def mySliceDef(i:Int) = {
      val doIt = i % 11 == 0
      if (doIt)
        println("Slice!")
      doIt
    }
    val sliceOn = counter.filter(mySliceDef)
    out("Slice")(sliceOn)

    // direct evenOdd -> bucket.x and div5 -> bucket.y
    val buckets = evenOdd.deriveSliced(k => new XYCollector).reduce().
      join(evenOdd){b => b.addX}.
      join(div5){b => b.addY}.
      slice_pre( sliceOn )

    out("in")(counter)
    out("buckets")(buckets)
  }

}
