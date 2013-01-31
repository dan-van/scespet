package scespet.core

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:10
 * To change this template use File | Settings | File Templates.
 */
class VectTerm[K,X](val eval:FuncCollector)(input:VectorStream[K,X]) {
  def map[Y](f:X=>Y):VectTerm[K,Y] = ???
  def map[Y <: Reduce[X]](y:Y):VectTerm[K,Y] = ???
//  def bucket[Y <: Reduce[X]](y:Y, window:Window = null):VectTerm[K,Y] = ???
}
