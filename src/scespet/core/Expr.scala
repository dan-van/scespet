package scespet.core

/**
 * This version works. core2 is the playground
 */

import java.util.concurrent.TimeUnit
import stub.gsa.esg.mekon.core.{Function => MFunc, Environment, EventGraphObject}
import dan.{VectorCollapse, FunctionVector, VectorStream, AbstractVectorStream}
import collection.mutable
import stub.gsa.esg.mekon.core
import dan.VectorStream.ReshapeSignal
import reflect.ClassTag
import scespet.core.AbsFunc

/**
 * @version $Id$
 */


trait FuncCollector {
  def bind(src:EventGraphObject, sink:MFunc)
  def env:Environment
}


/**
 * Something that provides a value (i.e. a source)
 * @tparam X
 */
trait HasVal[X] {
  def value:X
  def trigger :EventGraphObject
}

/**
 * something that has a source, and is a source (i.e. a pipe)
 * on "calculate", it may take from its source, and update its yielded value
 * if calculate returns false, then state was not modified
 *
 * i.e. this is an operation that can both map and filter
 * @tparam X
 * @tparam Y
 */
trait Func[X,Y] extends HasVal[Y] with MFunc {
  var source:HasVal[X]
  def value:Y
  def trigger = this
  def calculate():Boolean
}

trait Fold[X] {def add(x:X):Unit}

/**
 * this represents a binding of source -> sink
 * it provides operations to build subsequent bindings (e.g. "map")
 * it notifies a FuncCollector about all constructed bindings
 *
 * @param source
 * @param collector
 * @tparam X
 */
class Expr[X](val source:HasVal[X])(implicit collector:FuncCollector) {
//  implicit def selectToFunc[Y <: Select[X]](select:Y) = {new SelectFunc[X,Y](select)}

  def map[Y](f:Func[X,Y]):Expr[Y] = {
    f.source = source
    collector.bind(source.trigger, f)
    return new Expr[Y](f)
  }


  // TODO: structural types can't take generics, so I can't match on add(x:X). Damn
//  abstract class Reduce(implicit cTag:ClassTag[X]) {def add(x:X):Unit}
//  trait Reduce[R] {def r:R; def add(x:X):Unit}
//  implicit def toReduce[R <: {def add(x:X):Unit}]( obj:R ):Reduce[R] = {
//    return new Reduce[R] {
//      override val r:R = obj
////      def add(x: X){obj.add(x)}
//      def add(x: X) {obj.add(x)}
//    }
//  }
////  type Reduce[X] {def add(x:X):Any}
//
//  def reduce[M](m:Reduce[M]):Expr[M] = {
//    val funcWrap = new AbsFunc[X, M] {
//      override val value = m.r
//      def calculate() = {m.add( this.source.value ) ; true}
//    }
//    return map(funcWrap)
//  }

  def reduce[M <: Fold[X]](m:M):Expr[M] = {
    val funcWrap = new AbsFunc[X, M] {
      override val value = m
      def calculate() = {m.add( this.source.value ) ; true}
    }
    return map(funcWrap)
  }

  def map[Y](f:X => Y):Expr[Y] = map(new AnonFunc(f))

//  def sel(select:Select[X]):Expr[select.Self] = map(new SelectFunc[X,select.Self](select))
  def sel(select:Select[X]):Expr[select.Me] = {return new Expr[select.Me](new SelectFunc[X, select.Me](select.asInstanceOf[select.Me]))}

  def filter(f:X => Boolean):Expr[X] = map(new AnonFilterFunc[X](f))

  def sample(n:Long):Expr[X] = map(new SampledFunc[X](n))

  def foreach(f:X => Unit) = map(new AnonFunc(f))

  def group[Y](f:X => Y):VectorExpr[Y, X] = {
    val vFunc: GroupFunc[Y, X] = new GroupFunc[Y, X](source, f, collector.env)
    collector.bind(source.trigger, vFunc)
    return new VectorExpr[Y, X](vFunc)
  }

}

// ------- now the vector stuff ------------

class VectorExpr[K,X]( source:VectorStream[K,X])(implicit collector:FuncCollector){
  /**
   * generate a new derived vector, that uses "createFunc" to generate new vector cells.
   * BoundFunctionVector manages this, and we chain it up to this current expression's vector
   * @param createFunc
   * @tparam Y
   * @return
   */
  def mapEach[Y](createFunc: (K) => Func[X,Y]): VectorExpr[K, Y] = {
    val funcVector = new BoundFunctionVector[K, X, Func[X,Y], Y](createFunc, source, collector)
    funcVector.setSource(source)
    return new VectorExpr[K, Y](funcVector)
  }

  def mapf[Y](f:Func[X,Y]): VectorExpr[K, Y] = {
    val createFunc: (K) => Func[X,Y] = (_) => {f.getClass.newInstance()}
    return mapEach(createFunc)
  }

  type Copy[X,Y] = {def copy():Func[X,Y]}
  type CopyF[X,Y] = Func[X,Y] with Copy[X,Y]

  def map[Y](f:CopyF[X,Y]):VectorExpr[K,Y] = {
    val createFunc: (K) => Func[X,Y] = (_) => {
      f.copy()
    }
    return mapEach(createFunc)
  }


  def map[Y](f:X => Y):VectorExpr[K, Y] = {
    type YFunc = AnonFunc[X,Y]
    val createFunc = (key:K) => {
      new AnonFunc[X,Y](f){
        override def toString = "PerElement-map("+f+")"
      }
    }
    return mapEach(createFunc)
  }

  def filter(f:X => Boolean):VectorExpr[K,X] = {
    type YFunc = AnonFilterFunc[X]
    val createFunc = (key:K) => {
        new AnonFilterFunc[X](f){
        override def toString = "PerElement-filter("+f+")"
      }
    }
    return mapEach(createFunc)
  }

  def mapv[Y](f:VectorStream[K,X] => Y):Expr[Y] = {
    // func yields a Y,
    val func = new AnonFunc[VectorStream[K,X], Y](f) {
      override def calculate() = {
        super.calculate()
      }

      override def toString = "mapv("+f+")"
    }
    func.source = new IsVal[VectorStream[K,X]](source){
      // note, no trigger
      override def toString = "Vector source for collapse: "+source
    }
    new VectorCollapse(source, func, collector.env)

    // this presents those changing Y values and the associated listenable
    val ySource = new HasVal[Y] {
      def value = func.value
      def trigger = func
    }
    return new Expr[Y](ySource)
  }

  /**
   * converts a VectorExpr[K,V] -> Expr[ VectorStream[K,V] ]
   *
   * @param f
   * @tparam Y
   * @return
   */
  def pack[Y](f:VectorStream[K,X] => Y):Expr[Y] = {
    // func yields a Y,
    val func = new AnonFunc[VectorStream[K,X], Y](f) {
      override def calculate() = {
        super.calculate()
      }

      override def toString = "mapv("+f+")"
    }
    func.source = new IsVal[VectorStream[K,X]](source){
      // note, no trigger
      override def toString = "Vector source for collapse: "+source
    }
    new VectorCollapse(source, func, collector.env)

    // this presents those changing Y values and the associated listenable
    val ySource = new HasVal[Y] {
      def value = func.value
      def trigger = func
    }
    return new Expr[Y](ySource)
  }

  def reduce[M <: Fold[X]](m:M)(implicit man:ClassTag[M]):VectorExpr[K,M] = {
    val createFunc = (k:K) => {
      val newM:M = man.unapply(Unit).get
      new AbsFunc[X, M] {
        override val value = newM
        def calculate() = {m.add( this.source.value ) ; true}
      }
    }
    return mapEach(createFunc)
  }

  //  // todo:
//  def rekey[K2](f:K => K2):VectorExpr[K2,X]{}
//
//  def group[Y](f:X => Y):VectorExpr[(K,Y), X] = {
//    val vFunc: GroupFunc[Y, X] = new GroupFunc[Y, X](source, f)
//    collector.bind(source., vFunc)
//    return new VectorExpr[Y, X](source, vFunc)
//  }
}

class BoundFunctionVector[K,X, F <: Func[X,V], V](val createFunc:(K) => F, val sourceVect:VectorStream[K,X] ,val collector:FuncCollector) extends FunctionVector[K, F, V] {
  def newCell(i: Int, key: K):F = {
    val newInstance = createFunc.apply(key)
    val triggerI = sourceVect.getTrigger(i)
    collector.bind(triggerI, newInstance)
    newInstance.source = new HasVal[X](){
      def value = sourceVect.get(i)
      def trigger = triggerI
    }

      // initialise the value?
    newInstance.calculate()
    return newInstance
  }

  def get(i: Int):V = {
    var func = getTrigger(i)
    return func.value
  }

  override def setSource(source: VectorStream[K, _]) {
    super.setSource(source)
    collector.bind(source.getNewColumnTrigger, getNewColumnTrigger)
  }
}

//class VectorFunc[K,IN,V](val source:DataVector[K,IN], val f: IN => V) extends DataVector[K,V] {
//  def update() {
//    for (i <- source.changedIndicies) {
//      val nextVal = f.apply( source.getAt(i) )
//      setAt(i, nextVal)
//    }
//  }
//}

// this one uses pur function calls and tracks updated indicies.
// we could try a verison that uses wakeup nodes.
class GroupFunc[K,V](source:HasVal[V], keyFunc:V => K, env:Environment) extends AbstractVectorStream[K,ValueFunc[V], V] with MFunc {

  val getNewColumnTrigger = new ReshapeSignal()

  def newCell(i: Int, key: K) = new ValueFunc[V](source.value, env)  //todo: may not be necessary to do source.value, init later?

  def get(i: Int) = getTrigger(i).value

  def calculate():Boolean = {
    val nextVal: V = source.value
    val key: K = keyFunc.apply(nextVal)
    var index: Int = getIndex(key)
    if (index == -1) {
      index = getSize()
      add(key)
      env.wakeupThisCycle(getNewColumnTrigger)
    }
    var func: ValueFunc[V] = getTrigger(index)
    func.setValue(nextVal)
    return true
  }
}

class ValueFunc[V](var value:V, env:Environment) extends MFunc {
  def setValue(newVal:V) {
    this.value = newVal
    env.wakeupThisCycle(this);
  }
  def calculate() = true;
}

// integrated map and filter. map -> null implies filter
class AnonFunc[X,Y](val f:X => Y) extends AbsFunc[X,Y]{
  def calculate() :Boolean = {
    //    print("Computing "+f+" of "+source.value)
    value=f(source.value)
    return value != null
  }
}

class AnonFilterFunc[X](val f:X => Boolean) extends AbsFunc[X,X]{
  def calculate() = {
    if (f(source.value)) {
      value=source.value
      true
    } else {
      false
    }
  }
}

/**
 * wrap an X as a Func[X,X]
 * @tparam X
 */
class Identity[X]() extends AbsFunc[X,X]{
  def calculate() = {
    value = source.value
    true
  }
}


/**
 * expose every nth sample of X as a Func[X,X]
 * @tparam X
 */
class SampledFunc[X](val n:Long) extends AbsFunc[X,X]{
  var counter = 0L
  def calculate() = {
    counter += 1
    if (counter % n == 0) {
      value = source.value
      true
    } else {
      false
    }
  }
}

/**
 *
 */
class SelectFunc[IN, OUT <: Select[IN]](val select:OUT) extends Func[IN,OUT]{
  var source :HasVal[IN] = _
  def value = select

  def calculate() = {
    select.update()
    true
  }
}

class IsVal[X](val value:X) extends HasVal[X] {
  def trigger = value.asInstanceOf[EventGraphObject]
}


//abstract class Select[IN: TypeTag](implicit tag: TypeTag[IN]) extends DelayedInit {
abstract class Select[IN] extends DelayedInit {
  //    value = this
  var in:IN = _
  type Me <: Select[IN]

  var initBody:()=>Unit = _

  override def delayedInit(x: => Unit) {
    initBody = ()=>{x;}
  }

  def update() {
    initBody()
  }

//  def apply():SelectFunc[IN,this.type] = {
//    return new SelectFunc[IN, Select.this.type](this)
//  }
}