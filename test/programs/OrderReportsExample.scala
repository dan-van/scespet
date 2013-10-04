package programs

import data.Plot
import scala.collection.mutable.ArrayBuffer
import scespet.core._
import scespet.util._

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

  class OrderState extends Reduce[OrderEvent] {
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

  val impl: SimpleEvaluator = new SimpleEvaluator()
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
  impl.run(200)
}

object Test1 extends OrderReportsExample {
  def doBody() {
    Plot.plot(impl.asStream(priceFactory.getMids("MSFT.O")))
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
    val sumFillByName = orderEvents.filterType[Fill].map(fill => (orderIdToName(fill.orderId).value, fill)).by(_._1).map(_._2.qty).fold_all(new Sum[Int]).map(_.sum)
    Plot.plot (sumFillByName)
  }
}
object Test4 extends OrderReportsExample {
  def doBody() {
    val eventsById = orderEvents.by(_.orderId)
    val orderIdToName = eventsById.filterType[NewOrder]().map(_.stock)
    val orderIdToIsOpen = eventsById.map(!_.isInstanceOf[Terminate])
    val orderIdToTrades = orderIdToName.derive2((id,name) => priceFactory.getTrades(name))
    val accVolPerOrder = orderIdToTrades.map(_.qty).fold(new Sum[Double]).window(orderIdToIsOpen).map(_.sum)
    Plot.plot (accVolPerOrder)
  }
}
object TestN extends OrderReportsExample {
  def doBody() {

    val orderWindows = orderEvents.by(_.orderId).map(!_.isInstanceOf[Terminate])
    //  val orderIdToName = collection.mutable.HashMap[String, String]()
    //  orderEvents.filterType[NewOrder].map(o => orderIdToName.put(o.orderId, o.stock))
    //  def orderIdToWindow(id:String) = orderWindows.input.getValueHolder(orderWindows.input.getKeys.indexOf(id))
    //  val orderIdToTrades = orderEvents.by(_.orderId).filterType[NewOrder].joinf(o => priceFactory.getTrades(o.stock))
    val orderIdToTrades = orderEvents.by(_.orderId).filterType[NewOrder].derive2((k,v) => priceFactory.getTrades(v.stock))
    //  out ("ordId:trades ") { orderIdToTrades.reduceNoMacro( new Sum ).window( orderWindows ) }
    out ("ordId:trades ") { orderIdToTrades.map(_.qty.toInt).reduce( new Sum[Int] ).window( orderWindows ) }
    //  orderIdToTrades.map(_.value.qty.toInt).reduceNoMacro(new Sum).window( x => orderIdToWindow(x)  )
    //  orderIdToTrades.reduce(new Sum).window(id => orderWindows.get(id)) //todo
    //  out("Rand") {trades}
    //  out("OrderOpen") {orderWindows}
  }
}
