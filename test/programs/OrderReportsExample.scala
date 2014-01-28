package programs

import data.Plot
import scala.collection.mutable.ArrayBuffer
import scespet.core._
import scespet.util._
import scespet.EnvTermBuilder

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 30/05/2013
 * Time: 13:26
 * To change this template use File | Settings | File Templates.
 */
abstract class OrderReportsExample extends App {
  def doBody()

  trait OrderEvent {
    def time:Long
    def orderId:String
  }
  case class NewOrder(val time:Long, val orderId:String, val stock:String, val refPx:Double, val qty:Integer) extends OrderEvent
  case class Fill(val time:Long, val orderId:String, val px:Double, val qty:Int) extends OrderEvent
  case class Terminate(val time:Long, val orderId:String) extends OrderEvent

  val orderEventList = new ArrayBuffer[OrderEvent]
  orderEventList += NewOrder(0, "ord1", "MSFT", 101, 100)
  orderEventList += NewOrder(1000, "ord2", "IBM", 1.2, 100)
  orderEventList += Fill(20000, "ord2", 1.3, 10)
  orderEventList += Fill(21000, "ord2", 1.3, 10)
  orderEventList += Fill(22000, "ord1", 101.3, 20)
  orderEventList += Fill(25000, "ord1", 101.1, 50)
  orderEventList += Terminate(25000, "ord1")
  orderEventList += Fill(26000, "ord2", 1.4, 20)
  orderEventList += Terminate(27000, "ord2")

  class OrderState extends SelfAgg[OrderEvent] {
    var orderId:String = _
    var stock:String = _
    var orderQty:Integer = _
    var fillQty:Integer = 0
    var cashFlow:Double = 0
    var running:Boolean = true

    def add(x: OrderEvent) {
      x match {
        case n:NewOrder => {orderId = n.orderId; stock = n.stock}
        case f:Fill => {fillQty += f.qty; cashFlow += f.qty * f.px}
        case t:Terminate => {running = false}
      }
    }
  }

  implicit val env = new SimpleEnv
  val impl = EnvTermBuilder(env)
//  var eventStream = impl.query(IteratorEvents(orderEventList))
//  out("fills") { eventStream.by(_.orderId).filterType[Fill].map(_.qty.toInt) }
//  out("Net fill") {eventStream.by(_.orderId).filterType[Fill].map(_.qty.toInt).fold_all2(new Sum)}

//  out("Rand") {impl.query( newRandom _)}
  var priceFactory = new PriceFactory(impl.env)
//  var universe = impl.query(IteratorEvents(List("MSFT", "IBM", "AAPL")))
//  var books = universe.valueSet[String]().joinf( priceFactory.getBBO )
//  var trades = universe.valueSet().joinf( priceFactory.getTrades )
  val orderEvents = impl.asStream(IteratorEvents(orderEventList)((o, _) => o.time))
  doBody()
  env.run(200)
}

object Test1 extends OrderReportsExample {
  def doBody() {
    val msft = impl.asStream(priceFactory.getMids("MSFT.O"))
    val vod = impl.asStream(priceFactory.getMids("VOD.L"))
    Plot.plot(msft)
    Plot.plot(vod)
  }
}
object Test2 extends OrderReportsExample {
  def doBody() {
    out("New orders") (orderEvents.filterType[NewOrder]())
  }
}
object Test3 extends OrderReportsExample {
  def doBody() {
    val orderIdToName = orderEvents.filterType[NewOrder]().by(_.orderId).map(_.stock)
    val orderEventsByName = orderEvents.by(evt => orderIdToName.apply(evt.orderId).value)
    out("evt by name"){orderEventsByName}
//    val sumFillByName = orderEvents.filterType[Fill].map(fill => (orderIdToName(fill.orderId).value, fill)).by(_._1).map(_._2.qty).fold_all(new Sum[Int]).map(_.sum)
//    Plot.plot (sumFillByName)
  }
}
object Test4 extends OrderReportsExample {
  def doBody() {
    val eventsById = orderEvents.by(_.orderId)
    val orderIdToName = eventsById.filterType[NewOrder]().map(_.stock)
    val orderIdToIsOpen = eventsById.map(!_.isInstanceOf[Terminate])
    val orderIdToTrades = orderIdToName.keyToStream(id => priceFactory.getTrades( orderIdToName(id).value ))
    val accVolPerOrder = orderIdToTrades.map(_.qty).fold(new Sum[Double]).window(orderIdToIsOpen)
    Plot.plot (accVolPerOrder)
  }
}
object TestN extends OrderReportsExample {
  def doBody() {

    val eventsById = orderEvents.by(_.orderId)
    val orderActive = eventsById.map(!_.isInstanceOf[Terminate])
    val orderIdToStock = eventsById.filterType[NewOrder].map(_.stock)
    val orderIdToTrades = eventsById.keyToStream(id => {val stock = orderIdToStock(id).value; priceFactory.getTrades(stock)})
    out ("ordId:external Volume ") { orderIdToTrades.map(_.qty.toInt).reduce( new Sum[Int] ).window( orderActive ) }
  }
}

  object TestTradeBuckets extends OrderReportsExample {
  def doBody() {
    val universe = impl.asVector(List("MSFT"))
    val trades = universe.keyToStream(priceFactory.getTrades)
    val mids = universe.keyToStream(priceFactory.getMids)
    val tradeType = trades.join(mids).map({case (t,m) => if (t.price > m) {"Buy"} else if (t.price < m) {"Sell"} else "none"})
  }
}
