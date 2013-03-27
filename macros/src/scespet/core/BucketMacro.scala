package scespet.core
import reflect.macros.Context


/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:08
 * To change this template use File | Settings | File Templates.
 */
object BucketMacro {
  def bucket2Macro[X : c.WeakTypeTag, Y : c.WeakTypeTag](c: Context{type PrefixType=BucketTerm[X]})(bucketFunc: c.Expr[Y]) :c.Expr[BucketBuilder[X,Y]] = {
    import c.universe.reify
    //    ???
    reify{
      //      var realY = implicitly[c.TypeTag[X]].tpe
      //      type boundY = Y Reduce[X]
      //      type boundY = Reduce[X]
      //      type Y_ = Y with Reduce[X]
      val newBFunc:()=>Y = () => {bucketFunc.splice}
      val term:BucketTerm[X] = c.prefix.splice
      term.newBucketBuilder[Y](newBFunc)
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


//  def reduce[X,Y]
//  (c: Context)
//  (bucketFunc: c.Expr[Y], window: c.Expr[Window]): c.Expr[Term[Y]] = {
//    import c.universe._
//    var newBucketFunc = reify(() => {
//      val newBucket = bucketFunc.splice; println("constructed new reduce: " + newBucket); newBucket
//    })
//    // TODO: an impl that takes ()=>Bucket
//    null
//  }
}
