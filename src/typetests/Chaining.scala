package typetests

import typetests.Chaining.Term

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 09/12/2012
 * Time: 00:39
 * To change this template use File | Settings | File Templates.
 */
abstract class Chaining {
  def from[T:scala.reflect.ClassTag ](src:T):Term[T] = ???
}

object Chaining {
  trait TimeWindowBuild {
    def apply():TimeWindow
    def hours():TimeWindow = ???
    def between(start:String, end:String):TimeWindow = ???
  }

  implicit def intToTimeWindowBuild(i:Int):TimeWindowBuild = ???
  implicit def timeWindowToTimeWindowBuild(tw:TimeWindow):TimeWindowBuild = ???

  trait Window{}
  trait TimeWindow extends Window {}

  trait Reduce[X] {
    def add(x:X)
  }

  trait VectTerm[K,X] {
    def map[Y](f:X=>Y):VectTerm[K,Y] = ???
    def map[Y <: Reduce[X]](y:Y):VectTerm[K,Y] = ???
    def bucket[Y <: Reduce[X]](y:Y, window:Window = null):VectTerm[K,Y] = ???
  }

  trait Term[X] {
    def by[K](f:X=>K):VectTerm[K,X] = ???
    def map[Y](f:X=>Y):Term[Y] = ???
    def map[Y <: Reduce[X]](y:Y):Term[Y] = ???
    def bucket[Y <: Reduce[X]](y:Y, window:Window = null):Term[Y] = ???
  }
}