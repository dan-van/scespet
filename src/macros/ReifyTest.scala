/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 27/12/2012
 * Time: 12:07
 * To change this template use File | Settings | File Templates.
 */

import macros.Reflect1.Foo
import scala.reflect.runtime.{universe => ru}
import scala.tools.reflect.Eval

val exp = ru.reify(new Foo)
val builderExp = ru.reify(() => exp.splice)
val buildFunc = builderExp.eval
//scala.reflect.runtime.currentMirror