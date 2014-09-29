package scespet.core


import gsa.esg.mekon.core.EventGraphObject
import scespet.util.Logged


/**
 * todo: remove code duplication with SlicedReduce
 *  event wiring
 *
 * joinInputs--+
 *             |
 *         joinRendezvous -+-> nextReduce -> SlicedBucket
 *             |           \                  /
 *             |            +----------------+
 * sliceEvent -+----------------------------/
 *
 *
 */
 
abstract class SlicedBucket[C, OUT] extends UpdatingHasVal[OUT] with Logged {
  def addInputBinding[IN](in:HasVal[IN], adder:C=>IN=>Unit)
}

