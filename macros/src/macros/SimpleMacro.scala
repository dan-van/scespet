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
  def returnX[X : c.WeakTypeTag, TX : c.WeakTypeTag](c: Context{type PrefixType=Builder})(x: c.Expr[X]) :c.Expr[Wrapper[TX]] = {
    import c.universe.reify
    reify {
      val builder:Builder = c.prefix.splice
      builder.build[TX]()
    }
  }
}

trait Builder {
  def build[B]():Wrapper[B]
}

trait Wrapper[X]{
  def getX:X
}