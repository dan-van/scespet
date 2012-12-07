import scespet.core2.core._
import collection.mutable.ArrayBuffer

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 11/11/2012
 * Time: 00:22
 * To change this template use File | Settings | File Templates.
 */
case class Trade(var price:Double, var qty:Double)
var trades = new ArrayBuffer[Trade]()
trades += new Trade(1.12, 100)
trades += new Trade(2.12, 200)
trades += new Trade(3.12, 100)
trades += new Trade(4.12, 200)
trades += new Trade(5.12, 100)


var simple = new SimpleEvaluator()
val tradeStream : Expr[Trade] = simple.events(trades)

// this works.
tradeStream.sel2(new Select2[Trade]{def turnover=in.price * in.qty;var px = in.price})
  .map(x => println("Turn = "+x.turnover+" @ px: "+x.px))

//// and this doesn't when I remove the type parameter
//tradeStream.sel2(new Select2{def turnover=in.price * in.qty;var px = in.price})
//  .map(x => println("Turn = "+x.turnover+" @ px: "+x.px))
//
//// which means that neither would this:
//tradeStream.sel2(new Select2{def turnover=in.price * in.qty;var px = in.price})
//  .sel2(new Select2{def turn2=in.turnover;var px2 = in.px})

// i.e. I can't 'chain' anonymous select statements.
// I wonder if I could use implicits, or 'dependent types' or new scala TypeClasses to resolve it

simple.run()