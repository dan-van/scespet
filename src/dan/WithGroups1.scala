import collection.mutable.ArrayBuffer
import scespet.core._
import stub.gsa.esg.mekon.core.{Function => MFunc}
//import scala.language.implicitConversions


/**
 * Experiments with approaches to getting Select statements
 */

case class Trade(var name:String, var price:Double, var qty:Double)
var trades = new ArrayBuffer[Trade]()
trades += new Trade("VOD", 1.12, 100)
trades += new Trade("VOD", 2.12, 200)
trades += new Trade("MSFT", 3.12, 100)
trades += new Trade("VOD", 4.12, 200)
trades += new Trade("MSFT", 5.12, 200)


class Sum extends AbsFunc[Double, Double]{
  def calculate() = {value = value + source.value; true}
}

class Avg extends AbsFunc[Double, Double] {
  def copy() = new Avg
  var n = 0
  var sum = 0.0
  def calculate() = {n = n + 1; sum = sum + source.value; value = sum / n; true}
}

def prints(x:Expr[_]):Unit = {
  x.map((it) => { println(it) } )
}

def prints(x:VectorExpr[_,_]):Unit = {
  x.mapv(x => {
    print("vector{")
    for (i <- 0 to x.getSize - 1) {
      print(x.getKey(i) + " -> " + x.get(i) +", ")
    }
    println("}")
  })
}

var simple = new SimpleEvaluator()
val tradeStream = simple.events(trades)

tradeStream.map( t => println("next trade: "+ t) )
val sumTrades = tradeStream.group( _.name ).map(_.qty).map( new Avg() ).mapv( x => {
  print("vector{")
  for (i <- 0 to x.getSize - 1) {
    print(x.getKey(i) + " -> " + x.get(i) +", ")
  }
  println("}")
} )

//var avgTrades = tradeStream.filter(_.name == "VOD").map(_.qty).map(new Avg()).map( it => {println(it)} )
//var avgTrades = tradeStream.group(_.name).map(_.qty).map(new Avg())
//prints(avgTrades)
// select avg qty from tradestream where name="VOD"

simple.run()

//var m = new Mekon("today")
//m.getTrades("VOD").map(_.price).map(_ * 100.0).map(x=>println("Price: " + x ))
//m.getPrices("VOD").map(_.price).map(_ * 100.0).map(x=>println("Price: " + x ))
//simple.run()
