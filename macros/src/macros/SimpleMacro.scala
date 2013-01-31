package macros

import reflect.macros.Context

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 30/01/2013
 * Time: 10:56
 * To change this template use File | Settings | File Templates.
 */
object SimpleMacro {
  def returnX[X : c.WeakTypeTag, TX : c.WeakTypeTag](c: Context{type PrefixType=TX with Builder})(x: c.Expr[X]) :c.Expr[Pair[X,TX]] = {
    import c.universe.reify
    reify {
      val builder = c.prefix.splice
      builder.build[X, TX](x.splice, builder.asInstanceOf[TX])
    }
  }

  def simpleReturnX[X : c.WeakTypeTag](c: Context{type PrefixType=Any})(x: c.Expr[X]) :c.Expr[MacroApply[X]] = {
    import c.universe.reify
    reify {
      new MacroApply[X](x.splice)
    }
  }
}

class MacroApply[T](val t:T) {
  import language.experimental.macros
  def doMacro[X](x:X):MacroApply[X] = macro SimpleMacro.simpleReturnX[X]

  override def toString = "MacroApply:"+t
}

trait Builder {
  def build[X,B](x:X, b:B):Pair[X,B]

// no - macro def cannot be abstract
//  def doMacro[T](x:T):Pair[T,Foo]
}

trait Wrapper[X]{
  def getX:X
}