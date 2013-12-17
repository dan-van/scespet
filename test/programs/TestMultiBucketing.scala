package programs

import collection.mutable.ArrayBuffer
import data.Plot
import scespet.core._
import scespet.util._
import java.util.{List => JList}
import collection.JavaConversions._
import scespet.core.types.MFunc


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

  test("bucket sliced reduce pre") {
    val stream = generateCounterAndPartialBucketDef(reduce = true, 26).slicePre()
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

  test("bucket sliced reduce post") {
    val stream = generateCounterAndPartialBucketDef(reduce = true, 26).slicePost()
    val recombinedEventCount = stream.mapVector(_.getValues.foldLeft[Int]( 0 ) ((c, bucket) => c + bucket.countBoth + bucket.countX + bucket.countY))
//    new StreamTest("Recombined count", List(11, 11, 5), recombinedEventCount)


    val evenBuckets = stream("Even")
    val expectedEven = List(
    {val v = new XYCollector; v.firstX =  0; v.lastX = 10; v.countX = 4; v.countBoth = 2;v.done=true; v}, // for i 0 -> 10, there are 3 even numbers and 2 (even & multiple of 5)
    {val v = new XYCollector; v.firstX = 12; v.lastX = 22; v.countX = 5; v.countBoth = 1;v.done=true; v}, // for i 12 -> 21, there are 3 even numbers and 2 (even & multiple of 5)
    {val v = new XYCollector; v.firstX = 24; v.lastX = 26; v.countX = 2; v.countBoth = 0;v.done=true; v}  // for i 22 -> 26, there are 3 even numbers and 2 (even & multiple of 5)
    )
    new StreamTest("Even.x", expectedEven, evenBuckets)

    val oddBuckets = stream("Odd")
    val expectedOdd = List(
    {val v = new XYCollector; v.firstX =  1; v.lastX = 11; v.countX = 5; v.countBoth = 1; v.done=true; v}, // for i 0 -> 10, there are 3 even numbers and 2 (even & multiple of 5)
    {val v = new XYCollector; v.firstX = 13; v.lastX = 21; v.countX = 4; v.countBoth = 1; v.done=true; v}, // for i 11 -> 21, there are 3 even numbers and 2 (even & multiple of 5)
    {val v = new XYCollector; v.firstX = 23; v.lastX = 25; v.countX = 1; v.countBoth = 1; v.done=true; v}  // for i 22 -> 26, there are 3 even numbers and 2 (even & multiple of 5)
    )
    new StreamTest("Odd", expectedOdd, oddBuckets)
  }


  ignore("bucket sliced fold pre") { //See docs on SliceBeforeBucket for why I can't support this yet
    val stream = generateCounterAndPartialBucketDef(reduce = false, 26).slicePre()
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

  // fails due to missing event after slice
  test("bucket sliced fold post") {
    val stream = generateCounterAndPartialBucketDef(reduce = false, 26).slicePost()
    val recombinedCounter = stream.mapVector(v => {val V = v.getValues.map(_.lastX); if (V.isEmpty) 0 else V.max})
    new StreamTest("Recombined count", (0 to 26), recombinedCounter)

    // now populate an expected sequence of observed values:
    val expectedEven = new ArrayBuffer[XYCollector]()
    val expectedOdd = new ArrayBuffer[XYCollector]()
    def fillExpectations(i:Int, j:Int) {
      val vOdd = new XYCollector;
      val vEven = new XYCollector;
      for (x <- i to j) {
        val isEven = x % 2 == 0
        // mutate the last odd or even state accumulation
        val v = if (isEven) vEven else vOdd
        if (v.firstX < 0) v.firstX = x
        v.lastX = x
        if (x % 5 == 0) {
          v.countBoth += 1
        } else {
          v.countX += 1
        }
        // now clone and put the copy into expected
        val vCopy = v.clone()
        if (x == j) {
          vCopy.done = true
          // expect an event for the 'done' fire from the other group
          val otherGroup = if (isEven) expectedOdd else expectedEven
          val closeEvent = otherGroup.last.clone()
          closeEvent.done = true
          otherGroup.append(closeEvent)
        }
        (if (isEven) expectedEven else expectedOdd).append(vCopy)
      }
    }
    fillExpectations(0, 11)
    fillExpectations(12, 22)
    fillExpectations(23, 26)

    new StreamTest("Even", expectedEven, stream("Even"))
    new StreamTest("Odd", expectedOdd, stream("Odd"))
  }

  def generateCounterAndPartialBucketDef(reduce:Boolean, n:Int) = {
    // set up two vector streams that fire events, sometimes co-inciding, sometimes independent
    val counter = impl.asStream(IteratorEvents(0 to n)((x,i)=>i.toLong))
    val evenOdd = counter.by(c => if (c % 2 == 0) "Even" else  "Odd")
    val div5 = evenOdd.filter(_ % 5 == 0)

    val slicer = evenOdd.deriveSliced(k => new XYCollector)
    val sliceBuilder = if (reduce) slicer.reduce() else slicer.fold()
    val unslicedBuckets = sliceBuilder.
      join(evenOdd){b => b.addX}.
      join(div5){b => b.addY}

    new {
      private def mySliceDef(i:Int) = {
        val doIt = i > 0 && i % 11 == 0
        if (doIt)
          println("Slice!")
        doIt
      }
      lazy val sliceOn = counter.filter(mySliceDef)
      lazy val windowStream = {
        sliceOn.map(new Function1[Any, Boolean] {
          var windowOpen = true
          def apply(v1: Any): Boolean = {
            windowOpen = !windowOpen;
            println("Window edge. WindowOpen = "+windowOpen)
            windowOpen}
        })
      }
      def slicePre() = unslicedBuckets.slice_pre( sliceOn )
      def slicePost() = unslicedBuckets.slice_post( sliceOn )
      def window() = unslicedBuckets.window( windowStream )
    }
  }


  ignore("bucket windows reduce") {
    val stream = generateCounterAndPartialBucketDef(reduce = true, 26).window()

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

}
