package scespet.core

import scespet.core.VectorStream.ReshapeSignal
import gsa.esg.mekon.core.EventGraphObject
import scespet.core.MultiVectorJoin.BucketCell

/**
 * This takes a Stream and demultiplexes it into a VectorStream using a value -> key function
 *
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:14
 * To change this template use File | Settings | File Templates.
 */
// this one uses pur function calls and tracks updated indicies.
// we could try a verison that uses wakeup nodes.
class BucketJoin[K,V,B](val source:VectorStream[K,V], val joinFunc:B=>V=>Unit)

abstract class MultiVectorJoin[K, B <: Bucket](
                      sourceShape:VectorStream[K,_],
                      sourceJoins:List[BucketJoin[K, _, B]],
                      env:types.Env) extends AbstractVectorStream[K, B](env) with types.MFunc {
  /*
   * this is responsible for tracking seen keys in a source input vector, and binding any new input cells
   * into the bucket aggregation (using the defined input->bucketUpdate function)
   */
  class JoinedVectorState(joinDef:BucketJoin[K, _, B], var seenKeyCount:Int = 0) {
    def updateKeys() = {
      var updated = false
      val inVector = joinDef.source
      for (i <- seenKeyCount until inVector.getSize) {
        val k = inVector.getKey(i)
        val myIndex = indexOf(k)
        if (myIndex >= 0) {
          val bucketCell :SlicedBucket[B] = getValueHolder(myIndex)
          type X = Any
          val joinInputHasVal = inVector.getValueHolder(i).asInstanceOf[HasVal[X]]
          val bucketJoinDefinition = joinDef.asInstanceOf[BucketJoin[K, X, B]]
          val adder = bucketJoinDefinition.joinFunc
          bucketCell.addInputBinding(joinInputHasVal, adder)
          updated = true
        }
      }
      seenKeyCount = inVector.getSize
      updated
    }
  }

  var inputReshapeToState = Map[ReshapeSignal, JoinedVectorState]()

//  var inputReshapeSignals = Map[ReshapeSignal, VectorStream[K, _]]()
//  var inputToSeenKeyCount = Map[VectorStream[K, _], Int]()
  for (j <- sourceJoins) {
    val newColumnTrigger = j.source.getNewColumnTrigger
    val state = new JoinedVectorState(j)
    inputReshapeToState += newColumnTrigger -> state
    env.addListener(newColumnTrigger, this)
    state.updateKeys()
  }


  def calculate(): Boolean = {
    var updated = false
    import collection.JavaConverters.iterableAsScalaIterableConverter
    val triggers = env.getTriggers(this).asScala
    for (t <- triggers) {
      if (t.isInstanceOf[ReshapeSignal]) {
        val joinState = inputReshapeToState(t.asInstanceOf[ReshapeSignal])
        updated |= joinState.updateKeys()
      }
    }
    updated
  }

  override def getValueHolder(i: Int): SlicedBucket[B] = super.getValueHolder(i).asInstanceOf[SlicedBucket[B]]

  val getNewColumnTrigger = new ReshapeSignal(env) {
    var x_seenKeys = 0  // rename to thisSeenKeys

    val x_changeSignal = sourceShape.getNewColumnTrigger
    env.addListener(x_changeSignal, this)

    // we've just done some listener linkage, ripple an event after listeners established
    env.fireAfterChangingListeners(this)

    override def calculate():Boolean = {
      for (i <- x_seenKeys until sourceShape.getSize) {
        val newKey = sourceShape.getKey(i)
        add(newKey)
      }
      x_seenKeys = sourceShape.getSize()
      return super.calculate()
    }
  }

  def newCell(i: Int, key: K) = {
    val bucketCell = createBucketCell(key)
    var hadInitialInput = false
    // bind the cell up to listen to all its input bindings.
    for (joinDef <- sourceJoins) {
      val inVector = joinDef.source
      val index = inVector.indexOf(key)
      if (index >= 0) {
        type X = Any
        val joinInputHasVal = inVector.getValueHolder(i).asInstanceOf[HasVal[X]]
        val bucketJoinDefinition = joinDef.asInstanceOf[BucketJoin[K, X, B]]
        val adder = bucketJoinDefinition.joinFunc
        bucketCell.addInputBinding(joinInputHasVal, adder)
        if (joinInputHasVal.initialised) {
          hadInitialInput = true
        }
      }
    }
    // Initialisation - ah, didn't think of that, going to have to apply the current input value from each join source if
    // it is already initialised
    // however, do I need to worry about slice_pre or slice_post here? I don't think there's any implicit causal ordering
    // between new input cells becoming available, and other input cells firing.
    // for now, I'm close my eyes to this problem, and just check/express the behaviour in a unit test.
    if (hadInitialInput) {
      // cause the bucket to fire (it's new, so we have to do it after listeners are chained)
      env.fireAfterChangingListeners(bucketCell.value)
    }
    bucketCell
  }

  def createBucketCell(key:K) :BucketCell[B]
}

object MultiVectorJoin {
  trait BucketCell[B] extends HasVal[B] {
    def addInputBinding[X](in:HasVal[X], adder:B=>X=>Unit) :Boolean
  }
}