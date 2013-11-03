package scespet.core

import scespet.core.VectorStream.ReshapeSignal
import scala.collection.mutable

/**
 * This takes a Stream and demultiplexes it into a VectorStream using a value -> key function
 *
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/01/2013
 * Time: 21:14
 * To change this template use File | Settings | File Templates.
 */
// this one uses pur function calls and tracks updated indicies.
// we could try a verison that uses wakeup nodes.
class VectorJoin[K, K2, X, Y](xVect:VectorStream[K,X], yVect:VectorStream[K2,Y], env:types.Env, keyMap:K => K2, fireOnOther:Boolean = true) extends AbstractVectorStream[K, (X,Y)](env) {

  class CellTuple(val key:K) extends UpdatingHasVal[(X,Y)]{
    var xIndex = -1
    def bindToX() {
      xIndex = xVect.getKeys.indexOf(key)
      if (xIndex >= 0) {
        val xTrigger = xVect.getTrigger(xIndex)
        env.addListener(xTrigger, this)
      }
    }

    var yIndex = -1
    def bindToY(yi:Int) {
      if (yIndex == yi) return //already bound

      val yKey = yVect.getKey(yi)
      if (yIndex >= 0)
        throw new UnsupportedOperationException(s"keyMap function maps multiple keys in Y onto $key. Mapped keys: { ${yVect.getKey(yIndex)}, $yKey")
      yIndex = yi
      println(s"joined y:${yKey} (index $yIndex) and x:$key (index $xIndex)")
      if (fireOnOther) {
        env.addListener(yVect.getTrigger(yIndex), this)
      }
    }

    def calculate() = {
      var fire = false
      if (env.hasChanged( xVect.getTrigger(xIndex)) ) {
        println("x fired")
      }
      val xVal = if (xIndex >= 0 && (xVect.initialised(xIndex) || env.hasChanged( xVect.getTrigger(xIndex)) )) {
        // damn, strictly if the cell has changed, then xVect.initialised(xIndex) should be true
        // unfortunately there is no causality between the listener that updates initialised and this listener
        fire = true
        xVect.get(xIndex)
      } else {
        null.asInstanceOf[X]
      }
      val yVal = if (yIndex >= 0 && yVect.initialised(yIndex)) {
        if (fireOnOther && env.hasChanged( yVect.getTrigger(yIndex))) {
          fire = true
        }
        yVect.get(yIndex)
      } else {
        null.asInstanceOf[Y]
      }
      if (fire) {
        value = ( xVal , yVal)
        true
      } else false
    }

    var value:(X,Y) = null
  }


  val getNewColumnTrigger = new ReshapeSignal(env) {
    var x_seenKeys = 0
    var y_seenKeys = 0

    val x_changeSignal = xVect.getNewColumnTrigger
    env.addListener(x_changeSignal, this)

    val y_changeSignal = yVect.getNewColumnTrigger
    env.addListener(y_changeSignal, this)


    // we've just done some listener linkage, ripple an event after listeners established
    env.fireAfterChangingListeners(this)

    override def calculate():Boolean = {
      for (i <- x_seenKeys to xVect.getSize - 1) {
        val newKey = xVect.getKey(i)
        add(newKey)
      }
      x_seenKeys = xVect.getSize()

      val newYKeys:Map[K2, Int] = (for (yi <- y_seenKeys to yVect.getSize - 1) yield yVect.getKey(yi) -> yi).toMap
      if (!newYKeys.isEmpty) {
        // slow algo, may want to cache this
        for (x <- 0 to xVect.getSize - 1) {
          val key = xVect.getKey(x)
          val yKey = keyMap(key)
          val yIndex = newYKeys.get(yKey)
          if (yIndex.isDefined) {
            getValueHolder(x).asInstanceOf[CellTuple].bindToY(yIndex.get)
          }
        }
      }
      y_seenKeys = yVect.getSize()

      return super.calculate()
    }
  }

  def newCell(i: Int, key: K) = {
    val cell = new CellTuple(key)
    cell.bindToX()
    val yKey = keyMap(key)
    val yKeyIndex = yVect.indexOf(yKey)
    if (yKeyIndex >= 0) cell.bindToY(yKeyIndex)

    if (cell.xIndex >= 0 || cell.yIndex >= 0) {
      val fired = cell.calculate()
      // this cell is now initialised
      getNewColumnTrigger.newColumnAdded(i, fired)
      if (fired) setInitialised(i)
    }
    cell
  }
}
