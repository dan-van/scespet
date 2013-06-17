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
  orderEventList += NewOrder(1, "ord2", "VOD", 1.2, 100)
  orderEventList += Fill(10, "ord2", 1.3, 10)
  orderEventList += Fill(11, "ord2", 1.3, 10)
  orderEventList += Fill(12, "ord1", 101.3, 20)
  orderEventList += Fill(15, "ord1", 101.1, 50)
  orderEventList += Terminate(15, "ord1")
  orderEventList += Fill(16, "ord2", 1.4, 20)
  orderEventList += Terminate(15, "ord2")

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
  var universe = impl.query(IteratorEvents(List("MSFT", "IBM", "AAPL")))
  var books = universe.valueSet[String]().joinf( priceFactory.getBBO )

  out("Rand") {books}

  impl.run(20)
}
