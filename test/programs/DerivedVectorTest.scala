package programs

import gsa.esg.mekon.core.EventGraphObject
import org.scalatest.junit.{ShouldMatchersForJUnit, AssertionsForJUnit}
import org.scalatest.{OneInstancePerTest, BeforeAndAfterEach}
import scespet.{core, EnvTermBuilder}
import scespet.core._
import scespet.core.types._
import scespet.util.{ScespetTestBase, SliceAlign}

import scala.reflect.ClassTag

/**
  * I'm realising that just as the SliceTests took a much lower-level approach to testing, and allowed me to focus on bottom-up
  * correctness, I need to do a similar thing for the initialisation semantics of cells in a derived vector
  */
class DerivedVectorTest extends ScespetTestBase with BeforeAndAfterEach with OneInstancePerTest with AssertionsForJUnit with ShouldMatchersForJUnit {
  var env:SimpleEnv = _
  var impl:EnvTermBuilder = _

  override protected def beforeEach() {
    super.beforeEach()
    env = new SimpleEnv
    impl = EnvTermBuilder(env)
  }

  class MyCell(val key:String) extends Bucket with OutTrait[String] {
    private var _lastVal:String = "INITIAL"
    var calcCount = 0
    var mutationCount = 0

    override def open(): Unit = _lastVal = "INITIAL"

    override def calculate(): Boolean = {
      calcCount += 1
      true
    }

    def updateLastVal(newVal:String) = {
      mutationCount += 1
      _lastVal = newVal
    }
    override def value(): String = _lastVal
  }

//  type K = String
//  type OUT = String
//  type CELL = MyCell
//  type B = CELL
//  type SLICE = MFunc
//
//  val doMutable = true
//  val keyToCell: K => CELL = (k) => new MyCell(k)
//  val lifecycle = scespet.core.KeyToSliceCellLifecycle.getKeyToSliceLife(keyToCell, implicitly[ClassTag[CELL]], doMutable)
//
//  private var bindings = List[(VectTerm[K, _], (B => _ => Unit))]()
//  val slice = new SLICE() {
//    override def calculate(): Boolean = true
//  }
//  val sliceSpec = implicitly[SliceTriggerSpec[SLICE]]
//  val vectSliceSpec:VectSliceTriggerSpec[SLICE] = VectSliceTriggerSpec.sliceSpecToVectSliceSpec(sliceSpec)
//  val groupBuilder = new UncollapsedVectGroupWithTrigger(null, slice, SliceAlign.BEFORE, env, vectSliceSpec)
//  val reduceType = ReduceType.CUMULATIVE
//  val cellOut = implicitly[AggOut[B, OUT]]
//  val exposeEmpty = true
//
//  val derived = new DerivedVector[K, OUT] {
//    override def newCell(i: Int, k: K): UpdatingHasVal[OUT] = {
//      val cellLifecycle = lifecycle.lifeCycleForKey(k)
//      val bindingsForK :List[(HasVal[_], (B => _ => Unit))] = bindings.map(pair => pair._1.apply(k).input -> pair._2)
//      val cell :SlicedBucket[B,OUT] = groupBuilder.newBucket(i, k, reduceType, cellLifecycle, cellOut, bindingsForK, exposeEmpty)
//      cell
//    }
//
//    override def dependsOn: Set[EventGraphObject] = core.GroupedTerm2.this.dependsOn
//
//    override def toString: String = input.input + s"->Reduce[$reduceType with $lifecycle]"
//  }
//  input.newIsomorphicVector(derived)

  test("initialisation"){
    val empty = new MutableVector[String](env)
    val term = new VectTerm[String,String](env)(empty)
  }
}
