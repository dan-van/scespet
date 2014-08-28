package scespet.core

import scespet.core.SliceCellLifecycle.AggSliceCellLifecycle

/**
 * Created by danvan on 27/08/2014.
 */
trait CellAdder[C, X] {
  def addTo(c:C, x:X) :Unit
}
object CellAdder {
  class AggIsAdder[A <: Agg[X], X]() extends CellAdder[A, X] {
    override def addTo(c: A, x: X): Unit = c.add(x)
  }
  implicit def aggSliceToAdder[A, X](ev:A <:< Agg[X]) : CellAdder[A, X] = {
    type A2 = A with Agg[X]
    new AggIsAdder[A2, X]().asInstanceOf[CellAdder[A,X]]
  }
  implicit class AggSliceToAdder[A, X](ev:A <:< Agg[X]) extends CellAdder[A, X] {
    override def addTo(c: A, x: X): Unit = ev.apply(c).add(x)
  }
}

trait SliceCellLifecycle[C] {
  def newCell():C
  def reset(c:C)
  def closeCell(c:C)
  // NODEPLOY - I think we should have a callback on every 'row' e.g. updated
}


object SliceCellLifecycle {
  class CellSliceCellLifecycle[X, A <: Cell](newCellF: => A) extends SliceCellLifecycle[A]{
    override def newCell(): A = newCellF
    override def closeCell(c: A): Unit = {}
    override def reset(c: A): Unit = {}
  }


  implicit class AggSliceCellLifecycle[X, A <: Agg[X]](newCellF: () => A) extends SliceCellLifecycle[A] with CellAdder[A, X] {
    override def newCell(): A = newCellF()
    override def closeCell(c: A): Unit = {}
    override def reset(c: A): Unit = {}

    override def addTo(c: A, x: X): Unit = c.add(x)
  }

  abstract class BucketCellLifecycle[C <: Bucket] extends SliceCellLifecycle[C] {
    def newCell(): C

    override def reset(c: C): Unit = c.open()

    override def closeCell(c: C): Unit = c.complete()
  }

  class BucketCellLifecycleImpl[C <: Bucket](newBucket: => C) extends BucketCellLifecycle[C] {
    override def newCell(): C = newBucket
  }
}
