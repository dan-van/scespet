package programs

import collection.mutable.ArrayBuffer
import data.Plot
import scespet.core._
import scespet.util._
import java.util.{List => JList}
import collection.JavaConversions._
import scespet.core.types.MFunc
import scespet.EnvTermBuilder
import scala.collection.mutable


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
class TestMultiBucketing extends FunSuite with BeforeAndAfterEach with OneInstancePerTest with AssertionsForJUnit with ShouldMatchersForJUnit {

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

  // an 'MFunc' which tracks how many x, y, or both events have occured
//  class XYCollector() extends Bucket with OutTrait[XYCollector] with CellAdder[Int] {
  class XYCollector() extends Bucket with CellAdder[Int] with Cloneable {
    var firstX:Int = -1
    var lastX:Int = -1
    var xChanged ,yChanged = 0
    var countX, countY, countBoth = 0
    var done = false

    override def open(): Unit = {
      firstX = -1
      lastX = -1
      xChanged = 0
      yChanged = 0
      countX = 0
      countY = 0
      countBoth = 0
      done = false
    }

    override def complete() {
      if (done) throw new AssertionError("double call to done: "+this)
      done = true
    }

    def calculate(): Boolean = {
      if (xChanged > 0 && yChanged > 0) countBoth += 1
      else if (xChanged > 0) countX += 1
      else if (yChanged > 0) countY += 1
      xChanged = 0; yChanged = 0
      true
    }
    def add(x:Int) = addX(x)
    def addX(x:Int) = {xChanged += 1; lastX = x; if (firstX == -1) firstX = x}
    def addY(x:Int) = yChanged += 1

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

  ignore("bucket sliced reduce pre") {
    val stream = new generateCounterAndPartialBucketDef(26).slicedStream(SliceAlign.BEFORE).last()
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

  ignore("bucket sliced reduce post") {
    val stream = new generateCounterAndPartialBucketDef(26).slicedStream(SliceAlign.AFTER).last()
    val recombinedEventCount = stream.mapVector(_.getValues.foldLeft[Int]( 0 ) ((c, bucket) => c + bucket.countBoth + bucket.countX + bucket.countY))
    new StreamTest("Recombined count", List(12, 11, 4), recombinedEventCount)


    val evenBuckets = stream("Even")
    val expectedEven = List(
    {val v = new XYCollector; v.firstX =  0; v.lastX = 10; v.countX = 4; v.countBoth = 2;v.done=true; v}, // for i 0 -> 10, there are 3 even numbers and 2 (even & multiple of 5)
    {val v = new XYCollector; v.firstX = 12; v.lastX = 22; v.countX = 5; v.countBoth = 1;v.done=true; v}, // for i 12 -> 21, there are 3 even numbers and 2 (even & multiple of 5)
    {val v = new XYCollector; v.firstX = 24; v.lastX = 26; v.countX = 2; v.countBoth = 0;v.done=true; v}  // for i 22 -> 26, there are 3 even numbers and 2 (even & multiple of 5)
    )
    new StreamTest("Even.x", expectedEven, evenBuckets)

    val oddBuckets = stream("Odd")
    val expectedOdd = List(
    {val v = new XYCollector; v.firstX =  1; v.lastX = 11; v.countX = 5; v.countBoth = 1; v.done=true; v}, // for i 1 -> 11, there are 5 odd numbers and 1 (odd & multiple of 5)
    {val v = new XYCollector; v.firstX = 13; v.lastX = 21; v.countX = 4; v.countBoth = 1; v.done=true; v}, // for i 13 -> 21, there are 4 even numbers and 1 (odd & multiple of 5)
    {val v = new XYCollector; v.firstX = 23; v.lastX = 25; v.countX = 1; v.countBoth = 1; v.done=true; v}  // for i 23 -> 25, there are 1 even numbers and 1 (odd & multiple of 5)
    )
    new StreamTest("Odd", expectedOdd, oddBuckets)
  }

  def generateExpectedXYCollectorValues(r:Seq[Int]) :Seq[XYCollector] = {
    val v = new XYCollector
    val buf = new ArrayBuffer[XYCollector]()
    val iter = r.iterator
    while (iter.hasNext) {
      val x = iter.next()
      if (v.firstX < 0) v.firstX = x
      v.lastX = x
      if (x % 5 == 0) {
        v.countBoth += 1
      } else {
        v.countX += 1
      }
      // now clone and put the copy into expected
      val vCopy = v.clone()
      buf.append(vCopy)
    }
    buf
  }

  def addDoneEvent(buf:mutable.Buffer[XYCollector]) {
    val copy = buf.last.clone
    copy.done = true
    buf.append(copy)
  }

  def generateExpectedXYCollectorValues(i:Int, j:Int, expectedOdd:mutable.Buffer[XYCollector], expectedEven:mutable.Buffer[XYCollector]) {
    val oddRange = (i to j).filter(_ % 2 == 1)
    expectedOdd.appendAll( generateExpectedXYCollectorValues(oddRange) )
    val evenRange = (i to j).filter(_ % 2 == 0)
    expectedEven.appendAll( generateExpectedXYCollectorValues(evenRange) )

    val (lastBuf, prevLastBuf) = if (j % 2 == 0) { // last value in range was even
      (expectedEven, expectedOdd)
    } else {
      (expectedOdd, expectedEven)
    }
    // now mutate to 'done' the last value
    lastBuf.last.done = true
    // and add a 'done' copy to the other buffer
    addDoneEvent(prevLastBuf)
  }



  ignore("bucket fold, slice before") { //See docs on SliceBeforeBucket for why I can't support this yet
    val stream = new generateCounterAndPartialBucketDef(26).slicedStream(SliceAlign.BEFORE).last()
    val recombinedCounter = stream.mapVector(v => {val V = v.getValues.map(_.lastX); if (V.isEmpty) -1 else V.max})
    new StreamTest("Recombined count", (0 to 26), recombinedCounter)

    // now populate an expected sequence of observed values:
    val expectedEven = new ArrayBuffer[XYCollector]()
    val expectedOdd = new ArrayBuffer[XYCollector]()
    generateExpectedXYCollectorValues(0, 10, expectedOdd, expectedEven)
    generateExpectedXYCollectorValues(11, 21, expectedOdd, expectedEven)
    generateExpectedXYCollectorValues(22, 26, expectedOdd, expectedEven)

    new StreamTest("Even", expectedEven, stream("Even"))
    new StreamTest("Odd", expectedOdd, stream("Odd"))
  }

  // fails due to missing event after slice
  test("bucket fold, slice after") {
    val stream = new generateCounterAndPartialBucketDef(26).slicedStream(SliceAlign.AFTER).all()
    val recombinedCounter = stream.mapVector(v => {val V = v.getValues.map(_.lastX); if (V.isEmpty) -1 else V.max})
    new StreamTest("Recombined count", (0 to 26), recombinedCounter)

    // now populate an expected sequence of observed values:
    val expectedEven = new ArrayBuffer[XYCollector]()
    val expectedOdd = new ArrayBuffer[XYCollector]()
    generateExpectedXYCollectorValues(0, 11, expectedOdd, expectedEven)
    generateExpectedXYCollectorValues(12, 22, expectedOdd, expectedEven)
    generateExpectedXYCollectorValues(23, 26, expectedOdd, expectedEven)

    new StreamTest("Even", expectedEven, stream("Even"))
    new StreamTest("Odd", expectedOdd, stream("Odd"))
  }

  class generateCounterAndPartialBucketDef(n: Int) {
    // set up two vector streams that fire events, sometimes co-inciding, sometimes independent
    val counter = impl.asStream(IteratorEvents(0 to n)((x,i)=>i.toLong))
    val evenOdd = counter.by(c => if (c % 2 == 0) "Even" else  "Odd")
    val div5 = evenOdd.filter(_ % 5 == 0)
//    val keySet = evenOdd.toKeySet()

    //    evenOdd.reduce()
//    val slicer = keySet.keyToStream(k => new XYCollector)
//    val slicer = keySet.keyToStream(k => impl.streamOf3(new XYCollector).bind(evenOdd(k))(_.addX).bind(div5(k))(_.addY).all())
//    val sliceBuilder : SliceBuilder[String, _, XYCollector] = if (reduce) slicer.reduce() else slicer.fold()
//    val unslicedBuckets = sliceBuilder.
//      join(evenOdd){b => b.addX}.
//      join(div5){b => b.addY}

    private def mySliceDef(i:Int) = {
      val doIt = i > 0 && i % 11 == 0
      if (doIt)
        println("Slice!")
      doIt
    }
    lazy val sliceOn:MacroTerm[Int] = counter.filter(mySliceDef)
    // turn 'sliceOn into a window, each fire of sliceOn is an open or close edge of a window
    lazy val windowStream:MacroTerm[Boolean] = {
      sliceOn.fold_all(new Reducer[Any, Boolean] {
        var windowOpen = true
        def value = windowOpen
        def add(x: Any): Unit = {
          windowOpen = !windowOpen;
          println("Window edge. WindowOpen = "+windowOpen)
        }
      })
    }
    def slicedStream(align:SliceAlign):GroupedTerm2[String, XYCollector, XYCollector] = {
      val grouped = evenOdd.group(sliceOn, align)
      grouped.collapseK(k => new XYCollector).bind(div5)(_.addY)
    }
    def windowedStream():GroupedTerm2[String, XYCollector, XYCollector] = {
      val grouped = evenOdd.window(windowStream)
      grouped.collapseK(k => new XYCollector).bind(div5)(_.addY)
    }
  }


  test("bucket windows reduce") {
    val stream = new generateCounterAndPartialBucketDef(26).windowedStream().last()

    // This is similar to slice_pre - if the window close event is atomic with a value for the bucket, that value is deemed to be not-in the bucket
    // i.e. close comes first
    val evenBuckets = stream("Even")
    val expectedEven = List(
    {val v = new XYCollector; v.firstX =  0; v.lastX = 10; v.countX = 4; v.countBoth = 2;v.done=true; v}, // for i 0 -> 10, there are 3 even numbers and 2 (even & multiple of 5)
//    {val v = new XYCollector; v.firstX = 12; v.lastX = 20; v.countX = 4; v.countBoth = 1;v.done=true; v}, // for i 12 -> 21, there are 3 even numbers and 2 (even & multiple of 5)
    {val v = new XYCollector; v.firstX = 22; v.lastX = 26; v.countX = 3; v.countBoth = 0;v.done=true; v}  // for i 22 -> 26, there are 3 even numbers and 2 (even & multiple of 5)
    )
    new StreamTest("Even.x", expectedEven, evenBuckets)

    val oddBuckets = stream("Odd")
    val expectedOdd = List(
    {val v = new XYCollector; v.firstX =  1; v.lastX =  9; v.countX = 4; v.countBoth = 1; v.done=true; v}, // for i 0 -> 10, there are 3 even numbers and 2 (even & multiple of 5)
//    {val v = new XYCollector; v.firstX = 11; v.lastX = 21; v.countX = 5; v.countBoth = 1; v.done=true; v}, // for i 11 -> 21, there are 3 even numbers and 2 (even & multiple of 5)
    {val v = new XYCollector; v.firstX = 23; v.lastX = 25; v.countX = 1; v.countBoth = 1; v.done=true; v}  // for i 22 -> 26, there are 3 even numbers and 2 (even & multiple of 5)
    )
    new StreamTest("Odd", expectedOdd, oddBuckets)
  }


  test("bucket window fold") {
    val stream = new generateCounterAndPartialBucketDef(26).windowedStream().last()
    val recombinedCounter = stream.mapVector(v => {val V = v.getValues.map(_.lastX); if (V.isEmpty) -1 else V.max})
    val counterExpectations = (0 to 10) ++ Seq(10) ++ (22 to 26)
    new StreamTest("Recombined count", counterExpectations, recombinedCounter)

    // now populate an expected sequence of observed values:
    val expectedEven = new ArrayBuffer[XYCollector]()
    val expectedOdd = new ArrayBuffer[XYCollector]()
    def doRange(r:Seq[Int]) {
      expectedEven.appendAll( generateExpectedXYCollectorValues( r.filter(_ % 2 == 0) ) )
      addDoneEvent(expectedEven)

      expectedOdd.appendAll( generateExpectedXYCollectorValues( r.filter(_ % 2 == 1) ) )
      addDoneEvent(expectedOdd)
    }

    doRange(0 to 10)
    // windows close on event 11, and open on event 22
    doRange(22 to 26)

    new StreamTest("Even", expectedEven, stream("Even"))
    new StreamTest("Odd", expectedOdd, stream("Odd"))
  }

}
