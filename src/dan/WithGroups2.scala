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
  def calculate() = {n += 1; sum += source.value; value = sum / n; true}
}

object Avg {
  def apply() = new Avg
}

class AvgFunc(source:HasVal[Double]) extends MFunc {
  var n = 0
  var sum = 0.0
  def calculate() = {n += 1; sum += source.value; true}
  def getAvg = sum / n
}

class AvgObj() {
  var n = 0
  var sum = 0.0
  def add(x:Double) {n += 1; sum += x}
  def getAvg = sum / n
}

def avg(expr:Expr[Double]):Expr[Double] = {
  expr.reduce(new AvgObj() with Fold[Double] {}).map(_.getAvg)
}

def printv(x:VectorExpr[_,_]):Unit = {
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

//tradeStream.group(_.name).map(_.price).mapv(printv)
//tradeStream.filter(_.name == "VOD").map(_.qty).reduce(new AvgObj() with Fold[Double]).map(x => println(s"avg Trade = ${x.getAvg}"))
//implicit def toCreateFunc[K,F <: AbsFunc[_,_]](func:F)(implicit man:ClassTag[F]) : (K) => (F) = {
//  (_) => {man.unapply(Unit).get}
//}
//var avgTrades = tradeStream.filter(_.name == "VOD").map(_.qty).map(new Avg())
var avgTrades = tradeStream.group(_.name).map(_.qty).map(new Avg())
avgTrades.map(x => println(s"avg Trade = $x"))
printv(avgTrades)
//tradeStream.group(_.name).map(_.qty).mapf( new Avg ).map(x => println(s"avg Trade = $x"))
// select avg qty from tradestream where name="VOD"

simple.run()

//var m = new Mekon("today")
//m.getTrades("VOD").map(_.price).map(_ * 100.0).map(x=>println("Price: " + x ))
//m.getPrices("VOD").map(_.price).map(_ * 100.0).map(x=>println("Price: " + x ))
//simple.run()
