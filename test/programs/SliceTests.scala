package programs

import java.util.logging.Logger

import gsa.esg.mekon.MekonConfig.GraphFactory
import gsa.esg.mekon.SystemMode
import gsa.esg.mekon.core.{EventGraph, InstantTreeBuildingGraphWalker, DefaultEnvironment}
import gsa.esg.mekon.run.Mekon
import org.scalatest.junit.{ShouldMatchersForJUnit, AssertionsForJUnit}
import org.scalatest.{Suite, OneInstancePerTest, BeforeAndAfterEach}
import programs.SliceTests.Params
import scespet.EnvTermBuilder
import scespet.core.SliceCellLifecycle.{CellSliceCellLifecycle, MutableBucketLifecycle}
import scespet.core._
import scespet.core.types._
import scespet.util.{SliceAlign, ScespetTestBase}

import scala.collection.immutable.IndexedSeq

object SliceTests {

  case class Params(
                     /**
                       * with 'old style' the bucket itself is a Function, and makes its own calls to env.addListener
                       * This has complexities in relation to the bucket producing events that are concurrent with Slice (or have the wrong happens-before relation).
                       * tricky.
                       */
                     sourceAIsOldStyle:Boolean,
                     exposeEmpty:Boolean,
                     doMutable:Boolean)

  def apply(p:Params) = {
    val t = new SliceTests
    t.args = p
    t.rootSuite = false
    t
  }
}

/**
 * A lower level test of SliceAfterBucket
 * Rather than using the full stream api to observe integration test scenarios of the effect on data,
 * these are direct tests of a single instance of the SliceAfterBucket component (with correct associated graph wiring)
 */
class SliceTests extends ScespetTestBase with BeforeAndAfterEach with OneInstancePerTest with AssertionsForJUnit with ShouldMatchersForJUnit {
  val logger = Logger.getLogger(classOf[SliceTests].getName)

  var env:Env = _
  type GRAPH = {
    def acquireFireLock( evt: EventGraphObject )
    def fire( evt: EventGraphObject) : Unit
    def releaseFireLock( evt: EventGraphObject )
  }
  var graph : GRAPH = _
//  var graph : SlowGraphWalk = _
  var impl:EnvTermBuilder = _

  /**
    * the following is some Scalatest Foo to use this test class with different args, but have a 'default arg' that allows me to
    * right-click on a test and run it in intellij
    */
  var args: Params = Params(sourceAIsOldStyle = true, exposeEmpty = false, doMutable = false)
  var rootSuite = true
  val allArgs = {
    val bools = IndexedSeq(true, false)
    val all = for (b1 <- bools; b2 <- bools; b3 <- List(false)) yield Params(sourceAIsOldStyle = b1, exposeEmpty = b2, doMutable = b3)
    all.filter(_ != args) // don't include the 'default' instance
  }

  override def testNames: Set[String] = if (rootSuite) Set.empty else super.testNames
  override def suiteId: String = super.suiteId + (if (rootSuite) "" else args.toString)
  override def suiteName: String = super.suiteId + args.toString
  override def nestedSuites: IndexedSeq[Suite] = if (rootSuite) allArgs.map( p => SliceTests(p) ) else super.nestedSuites
  override def newInstance: Suite with OneInstancePerTest = {
    val instance = super.newInstance.asInstanceOf[SliceTests]
    instance.args = this.args
    instance.rootSuite = this.rootSuite
    instance
  }

  override protected def beforeEach() {
    super.beforeEach()
    val useMekon = true
    if (useMekon) {
      val mekon = new Mekon(SystemMode.TEST)
      mekon.lockWhenOutsideLoop(false)  // set this to false because the tests in this class are calling graph.fire directly rather than using the normal event manager.
      mekon.consumeAllEvents(true)
      val eventGraph = new InstantTreeBuildingGraphWalker
      mekon.setGraphFactory(new GraphFactory {
        override def newGraph(realtime: Boolean) = eventGraph
      })
      val runner = mekon.newRunner()
      env = runner.getEnvironment
      graph = new Object() {
        def acquireFireLock(evt:EventGraphObject): Unit = eventGraph.acquireFireLock(evt)
        def fire(evt:EventGraphObject): Unit = {
          val lock = !eventGraph.isCurrentThreadWithinFire
          if (lock) eventGraph.acquireFireLock(evt)
          eventGraph.fireNodeAndChildren(evt)
          if (lock) eventGraph.releaseFireLock(evt)
        }
        def releaseFireLock(evt:EventGraphObject): Unit = eventGraph.releaseFireLock(evt)
      }
    } else {
      env = new SimpleEnv
//      graph = env.asInstanceOf[SimpleEnv].graph
    }

    impl = EnvTermBuilder(env)

//    sourceAIsOldStyle = true
//    exposeEmpty = false
//    doMutable = false
  }

  override protected def afterEach(): Unit = {
    env.shutDown("End", null)
    super.afterEach()
  }


  /**
   * sourceA is bound via old-style mekon graph wiring,
   * sourceB is bound via a binding a datasource onto a mutable method of the OldStyleFundAppend
    *
    * @param reduceType
   * @param expected
   * @param triggerAlign
   * @return (sourceA, sourceB, slice)
   */
  def setupTestABSlice(reduceType:ReduceType, expected:List[List[Char]], triggerAlign:SliceAlign) = {
    graph.acquireFireLock(null)
    try {
      val sourceA: ValueFunc[Char] = new ValueFunc[Char](env)
      val sourceB: ValueFunc[Char] = new ValueFunc[Char](env)
      setupTestABSliceImpl(sourceA, sourceB, reduceType, expected, triggerAlign)
    } finally {
      graph.releaseFireLock(null)
    }
  }

  def setupTestABSliceImpl(sourceA :ValueFunc[Char], sourceB :ValueFunc[Char], reduceType:ReduceType, expected:List[List[Char]], triggerAlign:SliceAlign) = {
    type S = EventGraphObject                                                                     // NODEPLOY need to set up mutable tests
    type Y = OldStyleFuncAppend[Char]
    type OUT = OldStyleFuncAppend[Char]
    val slice = new SliceFire()
    val valueStreamForOldStyleEvents :ValueFunc[Char] = if (args.sourceAIsOldStyle) {
      sourceA
    } else {
      new ValueFunc[Char](env) // just bind it to a no-op
    }


    if (args.exposeEmpty && reduceType == ReduceType.LAST) {
      logger.info("I'm not bothering with exposeEmpty==true for reduce=LAST right now. Skipping test body")
    } else  if (args.exposeEmpty && reduceType == ReduceType.CUMULATIVE && args.sourceAIsOldStyle) {
      logger.info("I'm not bothering with exposeEmpty==true for reduce=CUMULATIVE when using sourceAIsOldStyle. Skipping test body")
    } else {
      val aggOut = implicitly[AggOut[Y, OUT]]
      val sliceSpec = implicitly[SliceTriggerSpec[S]]

      var otherBindings = List[(HasVal[_], (Y => _ => Unit))]()
      if (!args.sourceAIsOldStyle) {
        otherBindings :+= sourceA -> ((y: Y) => (c: Char) => y.append(c))
      }
      otherBindings :+= sourceB -> ((y:Y) => (c:Char) => y.append(c))
      val lifecycle = if (args.doMutable) {
        new MutableBucketLifecycle[OldStyleFuncAppend[Char]](() => new OldStyleFuncAppend[Char]( valueStreamForOldStyleEvents, env))
      } else {
        new CellSliceCellLifecycle[OldStyleFuncAppend[Char]](() => new OldStyleFuncAppend[Char]( valueStreamForOldStyleEvents, env))
      }
      val groupBuilder = new UncollapsedGroupWithTrigger(null, slice, triggerAlign, env, sliceSpec)

      val sliceBucket = groupBuilder.newBucket(reduceType, lifecycle, aggOut, otherBindings, args.exposeEmpty)

      env.addListener(sliceBucket, new MFunc() {
        var i = 0

        override def calculate(): Boolean = {
          val observed = sliceBucket.value.value
          if (i >= expected.length) throw new AssertionError("Too many events! Got " + observed)
          val expect = expected(i)
          expectResult(expect, s"sliceAfter: Event $i was not expected")(observed)
          println(s"sliceAfter: Observed event: $i \t $observed as expected")
          i += 1
          true
        }

        addPostCheck("Didn't observe all expected:") {
          assert(i == expected.length, s"not enough events observed out of expected: $expected")
        }
      })
    }
    (sourceA, sourceB, slice)
  }

  test("Concept sliceAfter CUMULATIVE") {
    val expected = if (args.exposeEmpty) {
      List("", "A", "AB", "ABC", /*slice*/"", "D" /*slice*/, ""/*slice*/).map(_.toCharArray.toList)
    } else {
      List("A", "AB", "ABC", "D").map(_.toCharArray.toList)
    }
    val reduceType = ReduceType.CUMULATIVE

    val (sourceA, sourceB, slice) = setupTestABSlice(reduceType, expected, SliceAlign.AFTER)

    setAndFire(sourceA, 'A')

    setAndFire(sourceA, 'B')

    //SLICE concurrent with sourceA firing C. sourceA.setValue has queued up a 'wakeup' so is concurrent with slice
    // . SliceAfter means C is added before the slice takes effect.
    setAndFireSlice(sourceA, 'C', slice)
//NODEPLOY     IN THE LINE ABOVE, SOURCE A GETS UNLINKED!
    setAndFire(sourceA, 'D')

    // a slice will expose and empty bucket
    fireSlice(slice)
  }

  test("Concept sliceAfter LAST") {
    val expected = List("ABC" /*slice*/ , "D" /*slice*/).map(_.toCharArray.toList)
    val reduceType = ReduceType.LAST
    val (sourceA, sourceB, slice) = setupTestABSlice(reduceType, expected, SliceAlign.AFTER)

    setAndFire(sourceA, 'A')

    setAndFire(sourceA, 'B')

    setAndFire(sourceA, 'C')
    //SLICE concurrent with sourceA firing C. SliceAfter means C is added before the slice takes effect.
    fireSlice(slice)

    setAndFire(sourceA, 'D')
    fireSlice(slice)

    //SLICE
    fireSlice(slice)
  }

  test("Concept sliceBefore CUMULATIVE") {
    val expected = if (args.exposeEmpty) {
      // NOTE: exposeEmpty doesn't currently expose the initial bucket value. This is asymmetric, but a pain in the ass
      List("", "A", "AB", "ABC", /*slice*/ "", /* then add */ "D" , /*slice and add atomically*/ "E", /*initial empty slice*/"", /*final empty slice*/ "").map(_.toCharArray.toList)
    } else {
      List("A", "AB", "ABC", /*slice then add, but skip empty*/ "D" , /*slice and add atomically*/ "E").map(_.toCharArray.toList)
    }
    val reduceType = ReduceType.CUMULATIVE
    val (sourceA, sourceB, slice) = setupTestABSlice(reduceType, expected, SliceAlign.BEFORE)

    setAndFire(sourceA, 'A')

    setAndFire(sourceA, 'B')

    setAndFire(sourceA, 'C')

    // SLICE this should fire an event - we have already seen 'C', and now we have a fresh new bucket (e.g. imagine an ACCVOL
    // reset at end of day
    fireSlice(slice)

    // get 'D' into a bucket
    setAndFire(sourceA, 'D')

    // now firing 'E' and slice concurrently, should result in emptying the bucket first, then firing just an E
    val newVal = 'E'
    setAndFireSlice(sourceA, newVal, slice)

    //SLICE - should generate an empty bucket
    fireSlice(slice)

    //SLICE - should generate another empty bucket
    fireSlice(slice)
  }


  def fireSlice(slice: SliceFire): Unit = {
    graph.acquireFireLock(null)
    graph.fire(slice)
    graph.releaseFireLock(null)
  }

  def setAndFireSlice(valueContainer: ValueFunc[Char], newVal: Char, sliceFunc: SliceFire): Unit = {
    graph.acquireFireLock(null)
    sliceFunc.enqueueSetter(valueContainer, newVal)
    graph.fire(sliceFunc)
    graph.releaseFireLock(null)
  }

  def setAndFire(valueContainer: ValueFunc[Char], newVal: Char): Unit = {
    graph.acquireFireLock(null)
    valueContainer.setValue(newVal)
    graph.fire(valueContainer.trigger)
    graph.releaseFireLock(null)
  }

  test("Concept sliceBefore LAST") { // cumulative slice before not implemented (is that still necessary?)
    val expected = if (args.exposeEmpty) {
      List("ABC", /*slice*/ "D" , /*slice*/ "").map(_.toCharArray.toList)
    } else {
      List("ABC", /*slice*/ "D" /*slice, but empty bucket*/).map(_.toCharArray.toList)
    }
    val reduceType = ReduceType.LAST
    val (sourceA, sourceB, slice) = setupTestABSlice(reduceType, expected, SliceAlign.BEFORE)

    setAndFire(sourceA, 'A')

    setAndFire(sourceA, 'B')

    setAndFire(sourceA, 'C')

    //SLICE
    fireSlice(slice)

    setAndFire(sourceA, 'D')

    fireSlice(slice)

    //SLICE
    fireSlice(slice)
  }

  /**
   * under the 'bind to an adder' method of updating cells, this test works.
   * if the cell itself subscribes to events, and one of those events coincides with the slice trigger
   * then because we are supposed to be doing "slice before data", then this is a violation.
   */
  test("sliceBefore with concurrent slice and new value") {
    val expected = List("A", /*slice*/ "BC").map(_.toCharArray.toList)
    val reduceType = ReduceType.LAST
    val (sourceA, sourceB, slice) = setupTestABSlice(reduceType, expected, SliceAlign.BEFORE)
    val sliceAndSourceA = new SliceFire() {
      var doSlice = false
      override def calculate(): Boolean = {
        super.calculate()
        val ret = doSlice
        doSlice = false
        ret
      }

      override def toString: String = "SliceAndSourceA:doSlice="+doSlice
    }
    // define sourceA -> sliceAndSourceA -> slice
    // this ensures that sourceA is "before" and "concurrent" with the trigger I use for this test
    env.addListener(sourceA, sliceAndSourceA)
    env.addListener(sliceAndSourceA, slice)

    setAndFire(sourceA, 'A')

    //SLICE concurrent with sourceA firing 'B' - in this test, Slice preceeds new values, so we expect
    // the bucket to close with just an A
    sliceAndSourceA.doSlice = true
    setAndFireSlice(sourceA, 'B', sliceAndSourceA)

    setAndFire(sourceA, 'C')

    //SLICE will now expose BC
    sliceAndSourceA.doSlice = true
    fireSlice(slice)
  }

  test("Concept joined sources") {
    val expected = if (args.exposeEmpty) {
      List("", "A", "AB", "ABC",  /*slice*/ "", "DD", /*slice*/"", "EE", /*slice*/ "", /*slice*/ "").map(_.toCharArray.toList)
    } else {
      List("A", "AB", "ABC",  /*slice*/ "DD", /*slice*/ "EE").map(_.toCharArray.toList)
    }
    val reduceType = ReduceType.CUMULATIVE

    val (sourceA, sourceB, slice) = setupTestABSlice(reduceType, expected, SliceAlign.AFTER)
    setAndFire(sourceA, 'A')

    setAndFire(sourceB, 'B')

    setAndFire(sourceA, 'C')

    //SLICE
    fireSlice(slice)

    // fire concurrently both inputs
    graph.acquireFireLock(null)
    sourceA.setValue('D')
    sourceB.setValue('D')
    graph.fire(sourceA.trigger)
    graph.releaseFireLock(null)

    //SLICE
    fireSlice(slice)

    // fire concurrently both inputs, and the slice event
    graph.acquireFireLock(null)
    sourceA.setValue('E')
    sourceB.setValue('E')
    graph.fire(slice)
    graph.releaseFireLock(null)
    // that will emit "EE", and then an empty bucket

    // fire another empty slice
    fireSlice(slice)

  }

  test("Concept joined sources LAST") {
    val expected = if (args.exposeEmpty) {
      List("ABC", "DD", "EE", "").map(_.toCharArray.toList)
    } else {
      List("ABC", "DD", "EE").map(_.toCharArray.toList)
    }
    val reduceType = ReduceType.LAST

    val (sourceA, sourceB, slice) = setupTestABSlice(reduceType, expected, SliceAlign.AFTER)
    setAndFire(sourceA, 'A')

    setAndFire(sourceB, 'B')

    setAndFire(sourceA, 'C')

    //SLICE
    fireSlice(slice)

    // fire concurrently both inputs
    graph.acquireFireLock(null)
    sourceA.setValue('D')
    sourceB.setValue('D')
    graph.fire(sourceA.trigger)
    graph.releaseFireLock(null)

    //SLICE
    fireSlice(slice)

    // fire concurrently both inputs, and the slice event
    graph.acquireFireLock(null)
    sourceA.setValue('E')
    sourceB.setValue('E')
    graph.fire(slice)
    graph.releaseFireLock(null)


    // empty slice
    fireSlice(slice)

  }

  test("joined sources slice is data") {
    val expected = if (args.exposeEmpty) {
      List("", "A", "AB", "ABC", /*slice*/ "", "DD", /*slice*/"", "EE", /*slice*/"").map(_.toCharArray.toList)
    } else {
      List("A", "AB", "ABC", "DD", "EE").map(_.toCharArray.toList)
    }
    val reduceType = ReduceType.CUMULATIVE
    val (sourceA, sourceB, slice) = setupTestABSlice(reduceType, expected, SliceAlign.AFTER)
    // link the slice to be coincident (and derived) from B
    env.addListener(sourceB.trigger, slice)

    val concurentFirer: SliceFire = new SliceFire()
    env.addOrdering(concurentFirer, sourceA.trigger)
    env.addOrdering(concurentFirer, sourceB.trigger)


    setAndFire(sourceA, 'A')

    setAndFire(sourceA, 'B')

    //Data & SLICE. i.e. 'C' should be integrated into the bucket, and the 'slice' effect should come after
    setAndFire(sourceB, 'C')

    // fire concurrently both inputs and the slice
    concurentFirer.enqueueSetter(sourceA, 'D')
    concurentFirer.enqueueSetter(sourceB, 'D')
    fireSlice(concurentFirer)

    // fire concurrently both inputs, and the slice event
    concurentFirer.enqueueSetter(sourceA, 'E')
    concurentFirer.enqueueSetter(sourceB, 'E')
    fireSlice(concurentFirer)

  }

  test("joined sources slice is data LAST") {
    val expected = List("ABC","DD", "EE").map(_.toCharArray.toList)
    val reduceType = ReduceType.LAST
    val (sourceA, sourceB, slice) = setupTestABSlice(reduceType, expected, SliceAlign.AFTER)
    // link the slice to be coincident (and derived) from B
    env.addListener(sourceB.trigger, slice)

    val concurentFirer: SliceFire = new SliceFire()
    env.addOrdering(concurentFirer, sourceA.trigger)
    env.addOrdering(concurentFirer, sourceB.trigger)


    setAndFire(sourceA, 'A')

    setAndFire(sourceA, 'B')

    //Data & SLICE. i.e. 'C' should be integrated into the bucket, and the 'slice' effect should come after
    setAndFire(sourceB, 'C')

    // fire concurrently both inputs and the slice
    concurentFirer.enqueueSetter(sourceA, 'D')
    concurentFirer.enqueueSetter(sourceB, 'D')
    fireSlice(concurentFirer)

    // fire concurrently both inputs, and the slice event
    concurentFirer.enqueueSetter(sourceA, 'E')
    concurentFirer.enqueueSetter(sourceB, 'E')
    fireSlice(concurentFirer)
  }


  test("Initialise") {
    val expected = List("AB").map(_.toCharArray.toList)
    val reduceType = ReduceType.LAST

    val a :ValueFunc[Char] = new ValueFunc[Char](env)
    val b :ValueFunc[Char] = new ValueFunc[Char](env)

    a.setValue('A')

    val (sourceA, sourceB, slice) = setupTestABSliceImpl(a, b, reduceType, expected, SliceAlign.AFTER)
    // link the slice to be coincident (and derived) from B
    graph.acquireFireLock(null)
    env.addListener(sourceB.trigger, slice)
    graph.fire(a.trigger)
    graph.releaseFireLock(null)

    setAndFire(sourceA, 'B')

    fireSlice(slice)

  }

  class OldStyleFuncAppend[X](in:HasVal[X], env:types.Env) extends Bucket {
    var value = Seq[X]()
    env.addListener(in.trigger, this)
    // see scespet.core.SlowGraphWalk.feature_correctForMissedFireOnNewEdge
//    if (env.hasChanged(in.trigger)) {
//      logger.warning("NODEPLOY experimental - should I be responsible for self-wake if already fired?")
//      env.wakeupThisCycle(this)
//    }
    private var closed = false
    //    if (in.initialised) {
    //      env.fireAfterChangingListeners(this) // do my initialisation
    //    }
    override def calculate(): Boolean = {
      if (!closed && env.hasChanged(in.trigger)) {
        append(in.value)
        true
      } else false
    }

    def append(x: X) {
      value :+= x
    }

    override def open(): Unit = value = Nil

    /**
     * called after the last calculate() for this bucket. e.g. a median bucket could summarise and discard data at this point
     * NODEPLOY - rename to Close
     */
    override def close(): Unit = {
      closed = true
      if (args.sourceAIsOldStyle) {
        env.removeListener(in.trigger, this)
      }
      super.close()
    }
  }

  class SliceFire extends MFunc {
    private var actions :List[ (ValueFunc[Any], Any) ] = List()
    def enqueueSetter[X](valueContainer:ValueFunc[X], newVal:X): Unit = {
      actions = actions :+ (valueContainer.asInstanceOf[ValueFunc[Any]] -> newVal)
    }
    override def calculate(): Boolean = {
      for ((container, newVal) <- actions) {
        container.setValue(newVal)
      }
      actions = Nil
      true
    }

    override def toString: String = "SLICE trigger"
  }
}


class TestExposeEmptyOldStyle extends SliceTests {
  this.args = new Params(sourceAIsOldStyle = false, exposeEmpty = true, doMutable = true)
  this.rootSuite = false
}
