package programs

import gsa.esg.mekon.core.EventGraphObject
import org.scalatest.junit.{ShouldMatchersForJUnit, AssertionsForJUnit}
import org.scalatest.{OneInstancePerTest, BeforeAndAfterEach}
import scespet.EnvTermBuilder
import scespet.core.SliceCellLifecycle.MutableBucketLifecycle
import scespet.core._
import scespet.core.types._
import scespet.util.ScespetTestBase

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

  test("Concept sliceAfter") {
    type S = EventGraphObject
    type Y = OldStyleFuncAppend[Char]
    type OUT = OldStyleFuncAppend[Char]
    val slice = new MFunc() {
      override def calculate(): Boolean = true
    }
    val sourceA :ValueFunc[Char] = new ValueFunc[Char](env)
    val sourceB :ValueFunc[Char] = new ValueFunc[Char](env)
    val aggOut = implicitly[AggOut[Y, OUT]]
    val sliceSpec = implicitly[SliceTriggerSpec[S]]
    //    val otherBindings =
    val lifecycle :SliceCellLifecycle[OldStyleFuncAppend[Char]] = new MutableBucketLifecycle[OldStyleFuncAppend[Char]](() => new OldStyleFuncAppend[Char]( sourceA, env))
    var otherBindings = List[(HasVal[_], (Y => _ => Unit))]()
    otherBindings :+= sourceB -> ((y:Y) => (c:Char) => y.append(c))
    val sliceAfter = new SliceAfterBucket[S, Y, OUT](aggOut, slice, lifecycle, ReduceType.CUMULATIVE, otherBindings, env, sliceSpec, exposeInitialValue = false)

    env.addListener(sliceAfter, new MFunc() {
      val expected = List("A", "AB", "ABC"/*slice*/, "D" /*slice*/, ""/*slice*/).map(_.toCharArray.toList)
      var i = 0
      override def calculate(): Boolean = {
        val observed = sliceAfter.value.value
        val expect = expected(i)
        expectResult(expect, s"sliceAfter: Event $i was not expected")(observed)
        i += 1
        true
      }
    })

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


  test("Concept joined sources") {
    type S = EventGraphObject
    type Y = OldStyleFuncAppend[Char]
    type OUT = OldStyleFuncAppend[Char]
    val slice = new MFunc() {
      override def calculate(): Boolean = true
    }
    val sourceA :ValueFunc[Char] = new ValueFunc[Char](env)
    val sourceB :ValueFunc[Char] = new ValueFunc[Char](env)
    val aggOut = implicitly[AggOut[Y, OUT]]
    val sliceSpec = implicitly[SliceTriggerSpec[S]]
    //    val otherBindings =
    val lifecycle :SliceCellLifecycle[OldStyleFuncAppend[Char]] = new MutableBucketLifecycle[OldStyleFuncAppend[Char]](() => new OldStyleFuncAppend[Char]( sourceA, env))
    var otherBindings = List[(HasVal[_], (Y => _ => Unit))]()
    otherBindings :+= sourceB -> ((y:Y) => (c:Char) => y.append(c))
    val sliceAfter = new SliceAfterBucket[S, Y, OUT](aggOut, slice, lifecycle, ReduceType.CUMULATIVE, otherBindings, env, sliceSpec, exposeInitialValue = false)

    env.addListener(sliceAfter, new MFunc() {
      val expected = List("A", "AB", "ABC", /*slice*/ "DD", /*slice*/ "EE", /*slice*/ "").map(_.toCharArray.toList)
      var i = 0
      override def calculate(): Boolean = {
        val observed = sliceAfter.value.value
        val expect = expected(i)
        expectResult(expect, s"sliceAfter: Event $i was not expected")(observed)
        println(s"sliceAfter: Observed event: $i \t $observed as expected")
        i += 1
        true
      }
    })

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
    type S = EventGraphObject
    type Y = OldStyleFuncAppend[Char]
    type OUT = OldStyleFuncAppend[Char]
    val sourceA :ValueFunc[Char] = new ValueFunc[Char](env)
    val sourceB :ValueFunc[Char] = new ValueFunc[Char](env)
    val slice = new MFunc() {
      override def calculate(): Boolean = true
    }
    env.addListener(sourceB.trigger, slice)

    val aggOut = implicitly[AggOut[Y, OUT]]
    val sliceSpec = implicitly[SliceTriggerSpec[S]]
    //    val otherBindings =
    val lifecycle :SliceCellLifecycle[OldStyleFuncAppend[Char]] = new MutableBucketLifecycle[OldStyleFuncAppend[Char]](() => new OldStyleFuncAppend[Char]( sourceA, env))
    var otherBindings = List[(HasVal[_], (Y => _ => Unit))]()
    otherBindings :+= sourceB -> ((y:Y) => (c:Char) => y.append(c))
    val sliceAfter = new SliceAfterBucket[S, Y, OUT](aggOut, slice, lifecycle, ReduceType.CUMULATIVE, otherBindings, env, sliceSpec, exposeInitialValue = false)

    env.addListener(sliceAfter, new MFunc() {
      val expected = List("A", "AB", "ABC", /*slice*/ "DD", /*slice*/ "EE").map(_.toCharArray.toList)
      var i = 0
      override def calculate(): Boolean = {
        val observed = sliceAfter.value.value
        val expect = expected(i)
        expectResult(expect, s"sliceAfter: Event $i was not expected")(observed)
        println(s"sliceAfter: Observed event: $i \t $observed as expected")
        i += 1
        true
      }
    })

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
