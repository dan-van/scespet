package programs

import gsa.esg.mekon.core.EventGraphObject
import org.scalatest.junit.{ShouldMatchersForJUnit, AssertionsForJUnit}
import org.scalatest.{OneInstancePerTest, BeforeAndAfterEach}
import scespet.EnvTermBuilder
import scespet.core.SliceCellLifecycle.{CellSliceCellLifecycle, MutableBucketLifecycle}
import scespet.core._
import scespet.core.types._
import scespet.util.{SliceAlign, ScespetTestBase}

/**
 * A lower level test of SliceAfterBucket
 * Rather than using the full stream api to observe integration test scenarios of the effect on data,
 * these are direct tests of a single instance of the SliceAfterBucket component (with correct associated graph wiring)
 */
class SliceTests extends ScespetTestBase with BeforeAndAfterEach with OneInstancePerTest with AssertionsForJUnit with ShouldMatchersForJUnit {
  var env:SimpleEnv = _
  var impl:EnvTermBuilder = _

  override protected def beforeEach() {
    super.beforeEach()
    env = new SimpleEnv
    impl = EnvTermBuilder(env)
  }

  /**
   * sourceA is bound via old-style mekon graph wiring,
   * sourceB is bound via a binding a datasource onto a mutable method of the OldStyleFundAppend
   * @param reduceType
   * @param expected
   * @param triggerAlign
   * @return (sourceA, sourceB, slice)
   */
  def setupTestABSlice(reduceType:ReduceType, expected:List[List[Char]], triggerAlign:SliceAlign, doMutable:Boolean = false) = {
    type S = EventGraphObject                                                                     // NODEPLOY need to set up mutable tests
    type Y = OldStyleFuncAppend[Char]
    type OUT = OldStyleFuncAppend[Char]
    val slice = new MFunc() {
      override def calculate(): Boolean = true
    }
    val sourceA :ValueFunc[Char] = new ValueFunc[Char](env)
    val sourceB :ValueFunc[Char] = new ValueFunc[Char](env)
    val valueStreamForOldStyleEvents :ValueFunc[Char] = new ValueFunc[Char](env)

    val aggOut = implicitly[AggOut[Y, OUT]]
    val sliceSpec = implicitly[SliceTriggerSpec[S]]

    var otherBindings = List[(HasVal[_], (Y => _ => Unit))]()
    otherBindings :+= sourceA -> ((y:Y) => (c:Char) => y.append(c))
    otherBindings :+= sourceB -> ((y:Y) => (c:Char) => y.append(c))

    val sliceBucket = triggerAlign match {
      case SliceAlign.AFTER if doMutable => {
        val lifecycle = new MutableBucketLifecycle[OldStyleFuncAppend[Char]](() => new OldStyleFuncAppend[Char]( valueStreamForOldStyleEvents, env))
        new SliceAfterBucket[S, Y, OUT](aggOut, slice, lifecycle, reduceType, otherBindings, env, sliceSpec, exposeInitialValue = false)
      }
      case SliceAlign.AFTER if !doMutable => {
        val lifecycle = new CellSliceCellLifecycle[OldStyleFuncAppend[Char]](() => new OldStyleFuncAppend[Char]( valueStreamForOldStyleEvents, env))
        new SliceAfterBucket[S, Y, OUT](aggOut, slice, lifecycle, reduceType, otherBindings, env, sliceSpec, exposeInitialValue = false)
      }
      case SliceAlign.BEFORE if doMutable => {
        val lifecycle = new MutableBucketLifecycle[OldStyleFuncAppend[Char]](() => new OldStyleFuncAppend[Char]( valueStreamForOldStyleEvents, env))
        new SliceBeforeBucket[S, Y, OUT](aggOut, slice, lifecycle, reduceType, otherBindings, env, sliceSpec, exposeInitialValue = false)
      }
      case SliceAlign.BEFORE if !doMutable => {
        val lifecycle = new CellSliceCellLifecycle[OldStyleFuncAppend[Char]](() => new OldStyleFuncAppend[Char]( valueStreamForOldStyleEvents, env))
        new SliceBeforeSimpleCell[S, Y, OUT](aggOut, slice, lifecycle, reduceType, otherBindings, env, sliceSpec, exposeInitialValue = false)
      }
    }

    env.addListener(sliceBucket, new MFunc() {
      var i = 0
      override def calculate(): Boolean = {
        val observed = sliceBucket.value.value
        val expect = expected(i)
        expectResult(expect, s"sliceAfter: Event $i was not expected")(observed)
        println(s"sliceAfter: Observed event: $i \t $observed as expected")
        i += 1
        true
      }
    })
    (sourceA, sourceB, slice, valueStreamForOldStyleEvents)
  }

  test("Concept sliceAfter CUMULATIVE") {
    val expected = List("A", "AB", "ABC"/*slice*/, "D" /*slice*/, ""/*slice*/).map(_.toCharArray.toList)
    val reduceType = ReduceType.CUMULATIVE
    val (sourceA, sourceB, slice, oldStyleInput) = setupTestABSlice(reduceType, expected, SliceAlign.AFTER)

    sourceA.setValue('A')
    env.graph.fire(sourceA.trigger)

    sourceA.setValue('B')
    env.graph.fire(sourceA.trigger)

    sourceA.setValue('C')
    env.graph.fire(sourceA.trigger)
    //SLICE concurrent with sourceA firing C. SliceAfter means C is added before the slice takes effect.
    env.graph.fire(slice)

    sourceA.setValue('D')
    env.graph.fire(sourceA.trigger)
    env.graph.fire(slice)

    //SLICE
    env.graph.fire(slice)
  }

  test("Concept sliceAfter LAST") {
    val expected = List("ABC"/*slice*/, "D" /*slice*/, ""/*slice*/).map(_.toCharArray.toList)
    val reduceType = ReduceType.LAST
    val (sourceA, sourceB, slice, oldStyleInput) = setupTestABSlice(reduceType, expected, SliceAlign.AFTER)

    sourceA.setValue('A')
    env.graph.fire(sourceA.trigger)

    sourceA.setValue('B')
    env.graph.fire(sourceA.trigger)

    sourceA.setValue('C')
    env.graph.fire(sourceA.trigger)
    //SLICE concurrent with sourceA firing C. SliceAfter means C is added before the slice takes effect.
    env.graph.fire(slice)

    sourceA.setValue('D')
    env.graph.fire(sourceA.trigger)
    env.graph.fire(slice)

    //SLICE
    env.graph.fire(slice)
  }

  test("Concept sliceBefore CUMULATIVE") {
    val expected = List("A", "AB", "ABC", /*slice*/ "D" , /*slice*/ "").map(_.toCharArray.toList)
    val reduceType = ReduceType.CUMULATIVE
    val (sourceA, sourceB, slice, oldStyleInput) = setupTestABSlice(reduceType, expected, SliceAlign.BEFORE)

    sourceA.setValue('A')
    env.graph.fire(sourceA.trigger)

    sourceA.setValue('B')
    env.graph.fire(sourceA.trigger)

    sourceA.setValue('C')
    env.graph.fire(sourceA.trigger)

    //SLICE (this should not fire an event, as the bucket close adds no information to CUMULATIVE)
    env.graph.fire(slice)

    sourceA.setValue('D')
    env.graph.fire(sourceA.trigger)

    env.graph.fire(slice)

    //SLICE
    env.graph.fire(slice)
  }


  test("Concept sliceBefore LAST") { // cumulative slice before not implemented (is that still necessary?)
    val expected = List("ABC", /*slice*/ "D" , /*slice*/ "").map(_.toCharArray.toList)
    val reduceType = ReduceType.LAST
    val (sourceA, sourceB, slice, oldStyleInput) = setupTestABSlice(reduceType, expected, SliceAlign.BEFORE)

    sourceA.setValue('A')
    env.graph.fire(sourceA.trigger)

    sourceA.setValue('B')
    env.graph.fire(sourceA.trigger)

    sourceA.setValue('C')
    env.graph.fire(sourceA.trigger)

    //SLICE
    env.graph.fire(slice)

    sourceA.setValue('D')
    env.graph.fire(sourceA.trigger)

    env.graph.fire(slice)

    //SLICE
    env.graph.fire(slice)
  }

  test("mutable bucket cant concurrent sliceBefore") { // mutable buckets can't do 'slice before' concurrently with a value being added
    val expected = List("A", /*slice*/ "BC").map(_.toCharArray.toList)
    val reduceType = ReduceType.LAST
    val (sourceA, sourceB, slice, oldStyleInput) = setupTestABSlice(reduceType, expected, SliceAlign.BEFORE)
    val sliceAndSourceA = new MFunc() {
      override def calculate(): Boolean = true
    }
    env.addListener(sliceAndSourceA, slice)
    env.addListener(sliceAndSourceA, sourceA)

    sourceA.setValue('A')
    env.graph.fire(sourceA.trigger)

    //SLICE concurrent with sourceA firing 'B' - in this test, Slice preceeds new values, so we expect
    // the bucket to close with just an A
    sourceA.setValue('B')
    env.graph.fire(sliceAndSourceA)

    sourceA.setValue('C')
    env.graph.fire(sourceA.trigger)

    //SLICE will now expose BC
    env.graph.fire(slice)
  }

  test("Concept joined sources") {
    val expected = List("A", "AB", "ABC", /*slice*/ "DD", /*slice*/ "EE", /*slice*/ "").map(_.toCharArray.toList)
    val reduceType = ReduceType.CUMULATIVE

    val (sourceA, sourceB, slice, oldStyleInput) = setupTestABSlice(reduceType, expected, SliceAlign.AFTER)
    sourceA.setValue('A')
    env.graph.fire(sourceA.trigger)

    sourceB.setValue('B')
    env.graph.fire(sourceB.trigger)

    sourceA.setValue('C')
    env.graph.fire(sourceA.trigger)

    //SLICE
    env.graph.fire(slice)

    // fire concurrently both inputs
    sourceA.setValue('D')
    sourceB.setValue('D')
    env.graph.fire(sourceA.trigger)

    //SLICE
    env.graph.fire(slice)

    // fire concurrently both inputs, and the slice event
    sourceA.setValue('E')
    sourceB.setValue('E')
    env.graph.fire(slice)

    // empty slice
    env.graph.fire(slice)

  }

  test("Concept joined sources LAST") {
    val expected = List("ABC", "DD", "EE", "").map(_.toCharArray.toList)
    val reduceType = ReduceType.LAST

    val (sourceA, sourceB, slice, oldStyleInput) = setupTestABSlice(reduceType, expected, SliceAlign.AFTER)
    sourceA.setValue('A')
    env.graph.fire(sourceA.trigger)

    sourceB.setValue('B')
    env.graph.fire(sourceB.trigger)

    sourceA.setValue('C')
    env.graph.fire(sourceA.trigger)

    //SLICE
    env.graph.fire(slice)

    // fire concurrently both inputs
    sourceA.setValue('D')
    sourceB.setValue('D')
    env.graph.fire(sourceA.trigger)

    //SLICE
    env.graph.fire(slice)

    // fire concurrently both inputs, and the slice event
    sourceA.setValue('E')
    sourceB.setValue('E')
    env.graph.fire(slice)

    // empty slice
    env.graph.fire(slice)

  }

  test("joined sources slice is data") {
    val expected = List("A", "AB", "ABC", /*slice*/ "DD", /*slice*/ "EE").map(_.toCharArray.toList)
    val reduceType = ReduceType.CUMULATIVE
    val (sourceA, sourceB, slice, oldStyleInput) = setupTestABSlice(reduceType, expected, SliceAlign.AFTER)
    // link the slice to be coincident (and derived) from B
    env.addListener(sourceB.trigger, slice)

    sourceA.setValue('A')
    env.graph.fire(sourceA.trigger)

    sourceA.setValue('B')
    env.graph.fire(sourceA.trigger)

    //Data & SLICE. i.e. 'C' should be integrated into the bucket, and the 'slice' effect should come after
    sourceB.setValue('C')
    env.graph.fire(sourceB.trigger)

    // fire concurrently both inputs and the slice
    sourceA.setValue('D')
    sourceB.setValue('D')
    env.graph.fire(sourceB.trigger)

    // fire concurrently both inputs, and the slice event
    sourceA.setValue('E')
    sourceB.setValue('E')
    env.graph.fire(sourceB)

  }

  test("joined sources slice is data LAST") {
    val expected = List("ABC","DD", "EE").map(_.toCharArray.toList)
    val reduceType = ReduceType.LAST
    val (sourceA, sourceB, slice, oldStyleInput) = setupTestABSlice(reduceType, expected, SliceAlign.AFTER)
    // link the slice to be coincident (and derived) from B
    env.addListener(sourceB.trigger, slice)

    sourceA.setValue('A')
    env.graph.fire(sourceA.trigger)

    sourceA.setValue('B')
    env.graph.fire(sourceA.trigger)

    //Data & SLICE. i.e. 'C' should be integrated into the bucket, and the 'slice' effect should come after
    sourceB.setValue('C')
    env.graph.fire(sourceB.trigger)

    // fire concurrently both inputs and the slice
    sourceA.setValue('D')
    sourceB.setValue('D')
    env.graph.fire(sourceB.trigger)

    // fire concurrently both inputs, and the slice event
    sourceA.setValue('E')
    sourceB.setValue('E')
    env.graph.fire(sourceB)

  }


  class OldStyleFuncAppend[X](in:HasVal[X], env:types.Env) extends Bucket {
    var value = Seq[X]()
    env.addListener(in.trigger, this)
    //    if (in.initialised) {
    //      env.fireAfterChangingListeners(this) // do my initialisation
    //    }
    override def calculate(): Boolean = {
      if (env.hasChanged(in.trigger)) {
        append(in.value)
        true
      } else false
    }

    def append(x: X) {
      value :+= x
    }

    override def open(): Unit = value = Nil
  }

}
