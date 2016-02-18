package scespet.core

import scespet.core.types.{MFunc, EventGraphObject}

/**
 * Created by danvan on 25/08/2014.
 */
class VectorToStream[K,V](in:VectorStream[K,V], env:types.Env) extends HasVal[(K,V)] with MFunc {
  private var _value :(K,V) = _
  var initialised: Boolean = false
  var valueWaiting = false
  private var cellListeners = List[Cell] ()

  private val cellAdder = new MFunc {
    private var lastCellAdded = -1

    val sourceCols = in.getNewColumnTrigger
    env.addListener(sourceCols, this)

    for (i <- 0 until in.getSize) {
      listenToCell(i)
    }

    override def calculate(): Boolean = {
      if (env.hasChanged(sourceCols)) {
        for (i <- lastCellAdded + 1 until in.getSize) {
          listenToCell(i)
        }
        true
      } else false
    }

    private def listenToCell(i:Int): Unit = {
      val c = new Cell(in.getKey(i), in.getValueHolder(i))
      cellListeners :+= c
      lastCellAdded = i
    }
  }
  env.addWakeupReceiver(cellAdder, this)

  override def value: (K, V) = _value
  override def calculate(): Boolean = {
    if (valueWaiting) {
      initialised = true
      valueWaiting = false
      true
    } else {
      false
    }
  }

  /**
   * @return the object to listen to in order to receive notifications of <code>value</code> changing
   */
  override def trigger: EventGraphObject = this


  private class Cell(k:K, inCell:HasValue[V]) extends MFunc {
    env.addListener(inCell.getTrigger, this)
    env.addListener(this, VectorToStream.this)
    if (inCell.initialised()) {
      env.wakeupThisCycle(this)
    }

    override def calculate(): Boolean = {
      enqueueCellValue()
      true
    }

    def enqueueCellValue() {
      if (valueWaiting) throw new UnsupportedOperationException("Two concurrent cell fires not supported from " + in)
      _value = (k, inCell.value())
      valueWaiting = true
    }
  }
}
