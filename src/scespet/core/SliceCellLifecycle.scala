package scespet.core

import scespet.core.SliceCellLifecycle.AggSliceCellLifecycle

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Created by danvan on 27/08/2014.
 */

trait CellOut[C,OUT] {
//  type O
  def out(c:C):OUT
}

object CellOut {
  class OutTraitToCellOut[T <: OutTrait[O],O]() extends CellOut[T,O] {
    override def out(c: T): O = c.value().asInstanceOf[O]
  }
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

  implicit def extendsSToOut[Y <: Set[_]](ev :Y <:< Set[_]) :CellOut[Y,Y] = {
    new CellOut[Y, Y]{
      override def out(c: Y): Y = c
    }
  }
  implicit def setToOut[X](set:collection.mutable.Set[X]) = new Ident[collection.mutable.Set[X]]
}

trait OutTrait[O] {
  def value():O
}

trait AggOut[A, O] {
  def out(a:A):O
}

// needed to prioritise implicit OutTrait => AggOut conversion
trait PriorityAggOut[A, O] extends AggOut[A,O]

class LowPriority {
  implicit def anyToIdent[A] = new AggOut[A,A] {
    override def out(a: A): A = a
  }
}

object AggOut extends LowPriority {
  implicit def hasOutTrait[T,O](implicit ev:T <:< OutTrait[O]) = new PriorityAggOut[T, O] {
    override def out(a: T): O = a.value()
  }
}


trait CellOut2[C] {
  type O
  def out(c:C):O
}

object CellOut2 {
  class Ident2[X] extends CellOut2[X] {
    type O = X
    override def out(c: X): X = c
  }

  // NODEPLOY delete me
  implicit def cell2ToOut[C <: Cell] = new CellOut2[C] {
    type O=C#OUT
    override def out(c: C): O = c.value
  }
  implicit object Set2ToOut extends CellOut2[Set[_]] {
    type O = Set[_]
    override def out(c: Set[_]): Set[_] = c
  }
  implicit def extendsToOut[Y <: Cell](ev :Y <:< Cell) :CellOut2[Y] = {
    new CellOut2[Y]{
      type O = Y#OUT
      override def out(c: Y): O = c.value
    }
  }

//  implicit object BufferToOut extends CellOut2[collection.mutable.ArrayBuffer[Char]] {
//    type O = collection.mutable.ArrayBuffer[Char]
//    override def out(c: O): O = c
//  }
//  implicit class BufferToOut[T <: collection.mutable.ArrayBuffer[Char]](ev:T=:=collection.mutable.ArrayBuffer[Char]) extends CellOut2[collection.mutable.ArrayBuffer[Char]] {
//    type O = collection.mutable.ArrayBuffer[Char]
//    override def out(c: O): O = c
//  }

  implicit def extendsSToOut2[Y](ev :Y =:= collection.mutable.HashSet[Char]) :CellOut2[collection.mutable.HashSet[Char]] = {
    new CellOut2[collection.mutable.HashSet[Char]]{
      type O = collection.mutable.HashSet[Char]
      override def out(c: O): O = c
    }
  }
//  implicit def setToOut2[X](set:collection.mutable.HashSet[X])  = new Ident2[collection.mutable.HashSet[X]]
}


trait CellAdder[-X] {
  def add(x:X)
}
object CellAdder {
  implicit def aggToAdder[X](agg:CellAdder[X]) :CellAdder[X] = agg
  implicit def setToAdder[X](set:collection.mutable.Set[X]) :CellAdder[X] = new CellAdder[X] {
    override def add(x: X): Unit = set.add(x)
  }

  implicit def bufferToAdder[X](set:collection.mutable.ArrayBuffer[X]) :CellAdder[X] = new CellAdder[X] {
    override def add(x: X): Unit = set.append(x)
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
