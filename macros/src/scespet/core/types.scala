package scespet.core

import reflect.macros.Context

package object types {

  type EventGraphObject = stub.gsa.esg.mekon.core.EventGraphObject
}
/**
 * Something that provides a value (i.e. a source)
 * @tparam X
 */
trait HasVal[X] {
  def value:X
  def trigger :types.EventGraphObject
}

trait BucketBuilder[+T] {
  def each(n:Int):T
}

trait Reduce[X] {
  def add(x:X)
}

trait BucketTerm {
  def newBucketBuilder[B,T](newB:()=>B):BucketBuilder[T]
}

object BucketMacro {
  def bucket2Macro[TY : c.WeakTypeTag, Y : c.WeakTypeTag](c: Context{type PrefixType=BucketTerm})(bucketFunc: c.Expr[Y]) :c.Expr[BucketBuilder[TY]] = {
    import c.universe._
    //    ???
    reify{
      //      var realY = implicitly[c.TypeTag[X]].tpe
      //      type boundY = Y Reduce[X]
      //      type boundY = Reduce[X]
      //      type Y_ = Y with Reduce[X]
      val newBFunc:()=>Y = () => {println("Building new");bucketFunc.splice.asInstanceOf[Y]}
      val term:BucketTerm = c.prefix.splice
      term.newBucketBuilder[Y, TY](newBFunc)
      //      val input:HasVal[X] = term.input
      //      val eval:FuncCollector = term.eval
      //      new BucketBuilderImpl[X, Y_](null, input, eval).asInstanceOf[BucketBuilder[Term[Y]]]
      //      new BucketBuilderImpl[X, Y_](null, null, null).asInstanceOf[BucketBuilder[Term[Y]]]
      //      ???
      //      val boundF : () => boundY = newBFunc.asInstanceOf[() => boundY]
      //      val builder = new BucketBuilderImpl(newBFunc.asInstanceOf[() => boundY], input, eval)
      //      builder.asInstanceOf[BucketBuilder[Term[Y]]]
    }
  }

//  def bucket3Macro[X : c.WeakTypeTag, Y : c.WeakTypeTag](c: Context{type PrefixType=MacroTerm[X]})(bucketFunc: c.Expr[Y]) :c.Expr[BucketBuilder[Term[Y]]] = {
//    import c.universe._
//    //    ???
//    reify{
//      //      var realY = implicitly[c.TypeTag[X]].tpe
//      //      type boundY = Y Reduce[X]
//      //      type boundY = Reduce[X]
//      //      type Y_ = Y with Reduce[X]
//      //      val newBFunc:()=>Y_ = () => {println("Building new");bucketFunc.splice.asInstanceOf[Y_]}
//      //      val term:MacroTerm[X] = c.prefix.value
//      //      val input:HasVal[X] = term.input
//      //      val eval:FuncCollector = term.eval
//      //      new BucketBuilderImpl[X, Y_](null, input, eval).asInstanceOf[BucketBuilder[Term[Y]]]
//      null
//      //      ???
//      //      val boundF : () => boundY = newBFunc.asInstanceOf[() => boundY]
//      //      val builder = new BucketBuilderImpl(newBFunc.asInstanceOf[() => boundY], input, eval)
//      //      builder.asInstanceOf[BucketBuilder[Term[Y]]]
//    }
//  }


//  def bucket[X,Y]
//  (c: Context)
//  (bucketFunc: c.Expr[Y], window: c.Expr[Window]): c.Expr[Term[Y]] = {
//    import c.universe._
//    var newBucketFunc = reify(() => {
//      val newBucket = bucketFunc.splice; println("constructed new bucket: " + newBucket); newBucket
//    })
//    // TODO: an impl that takes ()=>Bucket
//    null
//  }
}
