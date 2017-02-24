package scespet.core

import kotlin.jvm.functions.{Function1 => KFunction1}


/**
  * @author danvan
  * @version $Id$
  */
class KotImpl[X] extends Series[X] {
  override def value(): X = ???
//  override def filter(accept:KFunction1[X, Boolean]):Series[X] = ???


  override def filter(x$1: kotlin.jvm.functions.Function1[_ >: X, Boolean]): scespet.core.Series[X] = ???
//  override def map[Y](x$1: kotlin.jvm.functions.Function1[_ >: X, _ <: Y],x$2: JBoolean): scespet.core.Series[Y] = ???

  override def map[Y](x$1: kotlin.jvm.functions.Function1[_ >: X, _ <: Y]):Y = ???

//  override def reduce[Y, O](x$1: kotlin.jvm.functions.Function0[_ <: Y],x$2: kotlin.jvm.functions.Function1[_ >: Y, _ <: scespet.core.CellAdder[X]],x$3: scespet.core.AggOut[Y,O],x$4: kotlin.reflect.KClass[Y]): scespet.core.Series[O] = ???
//  override def scan[Y, O](x$1: kotlin.jvm.functions.Function0[_ <: Y],x$2: kotlin.jvm.functions.Function1[_ >: Y, _ <: scespet.core.CellAdder[X]],x$3: scespet.core.AggOut[Y,O],x$4: kotlin.reflect.KClass[Y]): scespet.core.Series[O] = ???
}
