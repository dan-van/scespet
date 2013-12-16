package scespet.core


import scespet.core.MultiVectorJoin.BucketCell
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
 
abstract class SlicedBucket[Y <: Bucket] extends UpdatingHasVal[Y] with BucketCell[Y] with Logged {
}

