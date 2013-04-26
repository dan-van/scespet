package typetests

import scespet.core._
import gsa.esg.mekon.core.{Environment, EventGraphObject, Function => MFunc}

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 09/12/2012
 * Time: 00:39
 * To change this template use File | Settings | File Templates.
 */
//abstract class Chaining {
//  def from[T:scala.reflect.ClassTag ](src:T):Term[T] = ???
//}
//

trait Window {
  def open:Boolean
}




//trait Term[X] {
//  def by[K](f:X=>K):VectTerm[K,X] = ???
//  def map[Y](f:X=>Y):Term[Y] = ???
//  def map[Y <: Reduce[X]](y:Y):Term[Y] = ???
////  def reduce[Y <: Reduce[X]](bucketFunc: Y):BucketBuilder[Term[Y]]
//  //    def reduce[Y <: Reduce[X]](y:Y, window:Window = null):Term[Y]
//  //    def bucket2[Y <: Reduce[X]](y:Y):BucketBuilder[Term[Y]]
//}


object Chaining {
  // todo: more refinement on the window building

  implicit class NSamplesTrigger[X,R <: Reduce[X]](n:Int) {
    def samples():NewBucketTriggerFactory[X,R] = {
      new NewBucketTriggerFactory[X, R] {
        def create(source: HasVal[X], reduce: R, env:Environment) = new NthEvent(n, source.trigger, env)
      }
    }
  }

  trait WindowBuild[R] {
    def newWindow(value: R): Window
  }

  implicit class IntWindowBuild(i:Int) {
    def apply():TimeWindow = ???
    def hours():TimeWindow = ???
    def samples():NSamplesWindow = new NSamplesWindow(i)
  }

  trait TimeWindow extends Window {}
  class NSamplesWindow(val N:Int) extends Window {
    var n:Int = 0
    def pointAdded = n += 1
    def open = n < N
  }
}