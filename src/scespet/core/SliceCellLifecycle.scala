package scespet.core

import gsa.esg.mekon.core.EventGraphObject
import scespet.core.types.MFunc

import scala.reflect.ClassTag

/**
 * Created by danvan on 27/08/2014.
 */

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

// this is very like HasVal[X], should they be related?
class MutableValue[X](x:X) extends CellAdder[X] with OutTrait[X] {
  private var _x:X = x
  override def add(x: X): Unit = _x = x

  override def value(): X = _x
}

trait SliceCellLifecycle[C] {
  def C_type :ClassTag[C]
  /**
   * create a new cell.
   * @return
   */
  def newCell():C

  /**
    * This is called to stop a cell updating.
    * @param c
    */
  def closeCell(c:C)
}


object SliceCellLifecycle {
  class CellSliceCellLifecycle[A](newCellF: () => A)(implicit val C_type:ClassTag[A]) extends SliceCellLifecycle[A]{
    // NODEPLOY make this aother variant rather than using if blocks
    val doClose =
      if (classOf[MFunc].isAssignableFrom(C_type.runtimeClass)) {
        // it must also be closeable:
        if (!classOf[AutoCloseable].isAssignableFrom(C_type.runtimeClass)) {
          throw new IllegalArgumentException(C_type.runtimeClass + " is a Function, therefore it must also implement AutoCloseable' ")
        }
        true
      } else {
        false
      }
    override def newCell(): A = newCellF()
    override def closeCell(c: A): Unit = {if (doClose) {
      c.asInstanceOf[AutoCloseable].close()
    }}
  }


  class MutableBucketLifecycle[B <: Bucket](newCellFunc: () => B)(implicit val b_type:ClassTag[B]) extends SliceCellLifecycle[B] {
    throw new AssertionError("I want to replace the concept of mutable buckets")

    lazy val cell = newCellFunc()

    override def C_type: ClassTag[B] = b_type

    /**
     * create a new cell.
     * @return
     */
    override def newCell(): B = {
      cell.open()
      cell
    }

    override def closeCell(c: B): Unit = {
      // just pause the cell to be resumed in next call to newCell (which will call open on the cell again)
      cell.pause()
    }

  }


  //  implicit class AggSliceCellLifecycle[X, A <: Agg[X]](newCellF: () => A) extends SliceCellLifecycle[A] {
//    override def C_type:ClassTag[A] = ???
//    override def newCell(): A = newCellF()
//    override def closeCell(c: A): Unit = {}
//    override def reset(c: A): Unit = {}
//  }

//  abstract class BucketCellLifecycle[C <: Bucket] extends SliceCellLifecycle[C] {
//    override def C_type:ClassTag[C] = ???
//
//    def newCell(): C
//
//    override def reset(c: C): Unit = {}
//
//    override def closeCell(c: C): Unit = c.complete()
//  }
//
//  class BucketCellLifecycleImpl[C <: Bucket](newBucket: => C) extends BucketCellLifecycle[C] {
//    override def newCell(): C = newBucket
//  }
}
