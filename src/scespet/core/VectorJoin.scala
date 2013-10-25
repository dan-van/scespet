package scespet.core

import scespet.core.VectorStream.ReshapeSignal

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
class VectorJoin[K, K2, X, Y](xVect:VectorStream[K,X], yVect:VectorStream[K2,Y], env:types.Env, keyMap:K2 => K) extends AbstractVectorStream[K, (X,Y)] {

  class CellTuple(val key:K) extends UpdatingHasVal[(X,Y)]{
    var xIndex = -1
    def bindToX() {
      xIndex = xVect.getKeys.indexOf(key)
      env.addListener(xVect.getTrigger(xIndex), this)
    }

    var yIndex = -1
    def bindToY(yKey:K2) {
      if (yIndex >= 0)
        throw new UnsupportedOperationException(s"keyMap function maps multiple keys in Y onto $key. Mapped keys: { ${yVect.getKey(yIndex)}, $yKey")
      yIndex = yVect.getKeys.indexOf( yKey )
      env.addListener(yVect.getTrigger(yIndex), this)
    }

    def calculate() = {
      value = ( if (xIndex >= 0) xVect.get(xIndex) else null.asInstanceOf[X] , if (yIndex >= 0) yVect.get(yIndex) else null.asInstanceOf[Y])
      true
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

    override def calculate():Boolean = {
      if (env.hasChanged(x_changeSignal)) {
        for (i <- x_seenKeys to xVect.getSize - 1) {
          val newKey = xVect.getKey(i)
          add(newKey)
          get(newKey).asInstanceOf[CellTuple].bindToX()
        }
        x_seenKeys = xVect.getSize()
      }
      if (env.hasChanged(y_changeSignal)) {
        for (i <- y_seenKeys to yVect.getSize - 1) {
          val newYKey = yVect.getKey(i)
          val newKey = keyMap(newYKey)
          add(newKey)
          get(newKey).asInstanceOf[CellTuple].bindToY(newYKey)
        }
        y_seenKeys = yVect.getSize()
      }
      return super.calculate()
    }
  }

  def newCell(i: Int, key: K) = {
    val cell = new CellTuple(key)
    if (cell.xIndex >= 0 || cell.yIndex >= 0) {
      cell.calculate()
      // this cell is now initialised
      getNewColumnTrigger.newColumnAdded(i, true)
    }
    cell
  }
}
