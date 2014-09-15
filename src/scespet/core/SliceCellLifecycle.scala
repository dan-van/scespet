package scespet.core

import scespet.core.SliceCellLifecycle.AggSliceCellLifecycle

/**
 * Created by danvan on 27/08/2014.
 */

trait CellOut[C,OUT] {
//  type O
  def out(c:C):OUT
}

object CellOut {
  class Ident[X] extends CellOut[X,X] {
//    type O = X
    override def out(c: X): X = c
  }

  // NODEPLOY delete me
  implicit object CellToOut extends CellOut[Cell, Cell#OUT] {
    override def out(c: Cell): Cell#OUT = c.value
  }
  implicit object SetToOut extends CellOut[Set[_], Set[_]] {
    override def out(c: Set[_]): Set[_] = c
  }
  implicit def extendsToOut[Y <: Cell](ev :Y <:< Cell) :CellOut[Y,Y#OUT] = {
    new CellOut[Y, Y#OUT]{
      override def out(c: Y): Y#OUT = c.value
    }
  }
  implicit def setToOut[X](set:collection.mutable.Set[X]) = new Ident[collection.mutable.Set[X]]
}

trait CellAdder[-X] {
  def add(x:X)
}
object CellAdder {
  implicit def aggToAdder[X](agg:CellAdder[X]) :CellAdder[X] = agg
  implicit def setToAdder[X](set:collection.mutable.Set[X]) :CellAdder[X] = new CellAdder[X] {
    override def add(x: X): Unit = set.add(x)
  }

//  class AggIsAdder[A <: Agg[X], X]() extends CellAdder[A, X] {
//    override def addTo(c: A, x: X): Unit = c.add(x)
//  }
//  implicit def aggSliceToAdder[A, X](ev:A <:< Agg[X]) : CellAdder[A, X] = {
//    type A2 = A with Agg[X]
//    new AggIsAdder[A2, X]().asInstanceOf[CellAdder[A,X]]
//  }
//  implicit class AggSliceToAdder[A, X](ev:A <:< Agg[X]) extends CellAdder[A, X] {
//    override def addTo(c: A, x: X): Unit = ev.apply(c).add(x)
//  }
}

trait SliceCellLifecycle[C] {
  def newCell():C
  def reset(c:C)
  def closeCell(c:C)
  // NODEPLOY - I think we should have a callback on every 'row' e.g. updated
}


object SliceCellLifecycle {
  class CellSliceCellLifecycle[X, A](newCellF: () => A) extends SliceCellLifecycle[A]{
    override def newCell(): A = newCellF()
    override def closeCell(c: A): Unit = {}
    override def reset(c: A): Unit = {}
  }


  implicit class AggSliceCellLifecycle[X, A <: Agg[X]](newCellF: () => A) extends SliceCellLifecycle[A] {
    override def newCell(): A = newCellF()
    override def closeCell(c: A): Unit = {}
    override def reset(c: A): Unit = {}
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
