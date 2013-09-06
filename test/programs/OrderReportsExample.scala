package programs

import scala.collection.mutable.ArrayBuffer
import scespet.core.{Reduce, IteratorEvents, SimpleEvaluator, SimpleEnv}
import scespet.util._

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 30/05/2013
 * Time: 13:26
 * To change this template use File | Settings | File Templates.
 */
object OrderReportsExample extends App {
  trait OrderEvent {
    def time:Long
    def orderId:String
  }
  case class NewOrder(val time:Long, val orderId:String, val stock:String, val refPx:Double, val qty:Integer) extends OrderEvent
  case class Fill(val time:Long, val orderId:String, val px:Double, val qty:Integer) extends OrderEvent
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
  val orderEvents = impl.query(IteratorEvents(orderEventList)((o, _) => o.time))
  val orderWindows = orderEvents.by(_.orderId).map(!_.isInstanceOf[Terminate])
//  val orderIdToName = collection.mutable.HashMap[String, String]()
//  orderEvents.filterType[NewOrder].map(o => orderIdToName.put(o.orderId, o.stock))
  //  def orderIdToWindow(id:String) = orderWindows.input.getValueHolder(orderWindows.input.getKeys.indexOf(id))
  //  val orderIdToTrades = orderEvents.by(_.orderId).filterType[NewOrder].joinf(o => priceFactory.getTrades(o.stock))
  val orderIdToTrades = orderEvents.by(_.orderId).filterType[NewOrder].takekv((k,v) => priceFactory.getTrades(v.stock))
//  out ("ordId:trades ") { orderIdToTrades.reduceNoMacro( new Sum ).window( orderWindows ) }
  out ("ordId:trades ") { orderIdToTrades.map(_.qty.toInt).reduce( new Sum ).window( orderWindows ) }
  //  orderIdToTrades.map(_.value.qty.toInt).reduceNoMacro(new Sum).window( x => orderIdToWindow(x)  )
  //  orderIdToTrades.reduce(new Sum).window(id => orderWindows.get(id)) //todo
  //  out("Rand") {trades}
//  out("OrderOpen") {orderWindows}

  impl.run(200)
}
