package scespet.core

import stub.gsa.esg.mekon.core.{EventGraphObject, Environment}

/**
 * This uses a source of data: input, aggregates events into buckets, and provides those buckets as a stream.
 * @param input
 * @param newBFunc
 * @param triggerBuilder
 * @param env
 * @tparam Y
 * @tparam X
 */
class BucketMaintainer[Y <: Reduce[X], X](input:HasVal[X], newBFunc:() => Y, triggerBuilder: NewBucketTriggerFactory[X, Y], env:Environment) extends AbsFunc[X, Y] {
  var nextBucket: Y = _
  var newBucketTrigger: EventGraphObject = null

  def calculate(): Boolean = {
    var closedBucket = false
    if (newBucketTrigger == null || env.hasChanged(newBucketTrigger)) {
      // TODO: distinguish between initial event?
      println(s"Starting new reduce. Old = $nextBucket, bucketTrigger = $newBucketTrigger")
      if (nextBucket != null) {
        value = nextBucket
        closedBucket = true
      }
      nextBucket = newBFunc.apply()
      val newTrigger = triggerBuilder.create(input, value, env)
      if (newTrigger != newBucketTrigger) {
        if (newBucketTrigger != null) {
          env.removeListener(newBucketTrigger, this)
        }
        env.addListener(newTrigger, this)
        newBucketTrigger = newTrigger
      }
    }
    if (env.hasChanged(input.trigger)) {
      nextBucket.add(input.value);
    }
    return closedBucket
  }
}
