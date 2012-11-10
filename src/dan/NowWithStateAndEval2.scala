import collection.mutable.ArrayBuffer
import scespet.core._

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 10/05/2012
 * Time: 22:52
 * To change this template use File | Settings | File Templates.
 */

case class Trade(var price:Double, var qty:Double)
var trades = new ArrayBuffer[Trade]()
trades += new Trade(1.12, 100)
trades += new Trade(2.12, 200)
trades += new Trade(3.12, 100)
trades += new Trade(4.12, 200)
trades += new Trade(5.12, 100)


class CumuSum extends AbsFunc[Double, Double]{
  def calculate() = {value = value + source.value; true}
}

var simple = new SimpleEvaluator()

val tradeStream = simple.add(trades)
//tradeStream.sel(new Select{def Me = This; var cash = in.price * in.qty; var quantity = in.qty})
val scaledPrices: Expr[Double] = tradeStream.map(x => {println(s"Got trade: $x"); x.price}).map(_ * 100.0).map(x => {println(s"Scaled: $x"); x})
scaledPrices.map(new CumuSum()).map(x => println("cumu sum: " + x))

simple.run()



//var m = new Mekon("today")
//m.getTrades("VOD").map(_.price).map(_ * 100.0).map(x=>println("Price: " + x ))
//m.getPrices("VOD").map(_.price).map(_ * 100.0).map(x=>println("Price: " + x ))
//simple.run()
