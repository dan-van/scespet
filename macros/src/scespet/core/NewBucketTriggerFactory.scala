package scespet.core

import stub.gsa.esg.mekon.core.{EventGraphObject, Environment}

/**
 *
 * @tparam X this is the source event type that is being added to the Reduce
 * @tparam R this is the Reduce function that is performing the reduction of X
 */
trait NewBucketTriggerFactory[X, R <: Reduce[X]] {
  def create(source:HasVal[X], reduce:R, env:Environment) : EventGraphObject
}
