package programs

import datavis.data.Plot
import scespet.core.types.MFunc
import scespet.core.{CellAdder, IteratorEvents}

/**
 * Created by danvan on 08/10/2014.
 */
object ParticipationStatsTest extends RealTradeTests {
  class OrderEvent(val time:Long, val orderId:String) {}
  case class New(override val time:Long, override val orderId:String, val symbol:String, orderQty:Int) extends OrderEvent(time, orderId)
  case class Fill(override val time:Long, override val orderId:String, fillQty:Int, fillPrice:Double) extends OrderEvent(time, orderId)
  case class Terminated(override val time:Long, override val orderId:String) extends OrderEvent(time, orderId)
  val baseTime = 1383057001000L
  val orderEvents = impl.asStream(new IteratorEvents[OrderEvent](
    List(
    new New(baseTime+0, "abc", "MSFT.O", 10000),
    new Fill(baseTime+1000, "abc", 100, 2.6),
    new Fill(baseTime+2000, "abc", 110, 2.7),
    new Fill(baseTime+3000, "abc", 120, 2.8),
    new Fill(baseTime+4000, "abc", 130, 2.7),
    new Terminated(baseTime+5000, "abc")
    ), (evt, i) => evt.time))

  class OrderState(val orderId:String) extends CellAdder[OrderEvent]{
    var newOrder:New = _
    var netFill = 0
    var netFillCash = 0.0
    var terminated = false
    def symbol = newOrder.symbol

    override def add(x: OrderEvent): Unit = {
      x match {
        case n:New => newOrder = n
        case f:Fill => {
          netFill += f.fillQty
          netFillCash += f.fillQty * f.fillPrice
        }
        case n:Terminated => terminated = true
      }
    }
  }
  val idToState = orderEvents.by(_.orderId).scan(new OrderState(_))
  val orderStates = idToState.toValueSet
  val universe = orderEvents.filterType[New].map(_.symbol).valueSet()
  val orderToTrades = orderStates.keyToStream(state => getTradeEvents(state.symbol))

  class Vwap extends CellAdder[Trade] {
    var sum = 0L
    var sumPx = 0.0
    override def add(x: Trade): Unit = {
      sum += x.quantity
      sumPx += x.quantity * x.price
    }
  }
  val orderToVwap = orderToTrades.scan(new Vwap)
  def isNeedingMoreVolume(vwap:Vwap,ordState:OrderState) = {
    vwap.sum * 0.10 < ordState.newOrder.orderQty
  }
  val orderIsNeedingVolume = orderToVwap.take(idToState, _.orderId).map(e => !e._2.terminated && isNeedingMoreVolume(e._1, e._2))
  val tenPctPx = orderToVwap.window(orderIsNeedingVolume).last
  scespet.util.out("10% px"){tenPctPx}
  env.run()
}

object PrintTradesTest extends RealTradeTests {
  val trades = getTradeEvents("MSFT.O")
  env.addListener(trades, new MFunc{
    override def calculate(): Boolean = {
      println("Trade at "+env.getEventTime)
      true
    }
  })
  env.run()
}