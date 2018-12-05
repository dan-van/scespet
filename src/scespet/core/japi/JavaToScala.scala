package scespet.core.japi

import java.util.function.Supplier

/**
  * @author danvan
  * @version $Id$
  */
object JavaSupplierSupport {

  @inline
  def asScala[T](jSupplier: Supplier[T]): Function0[T] = () => jSupplier.get()

  @inline
  def asScala[T,R](f1: _root_.java.util.function.Function[T,R]): Function1[T,R] = t => f1.apply(t)
}