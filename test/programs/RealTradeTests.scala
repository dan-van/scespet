package programs

import data.Plot
import scespet.core._
import java.io.{BufferedWriter, FileWriter, File}
import java.net.URL
import java.text.SimpleDateFormat
import scespet.util.{_}

//import org.msgpack._
//import org.msgpack.annotation.Message
//import org.msgpack.ScalaMessagePack._
import programs.Trade
import org.scalatest.time.Minutes
import scala.concurrent.duration.{Duration, TimeUnit}

// <- import MessagePack instance for scala

import scala.io.Source

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 31/10/2013
 * Time: 21:41
 * To change this template use File | Settings | File Templates.
 */
abstract class RealTradeTests extends App with Logged {
  val date="20131029"
  val dataRoot = new File("./testdata")
  //time	price	quantity	board	source	buyer	seller	initiator
  case class Trade(time:Long,	price:Double,	quantity:Long,	board:String,	source:String,	buyer:String,	seller:String,	initiator:String)

  //time	bid	bid_depth	bid_depth_total	offer	offer_depth	offer_depth_total
//  20131030T090001	515.1	300	300	518.9	300	300
  case class Quote(time:Long,	bid:Double,	bidDepth:Long, ask:Double, askDepth:Long)

  val impl: SimpleEvaluator = new SimpleEvaluator()
  implicit val env = impl.env

  var tickerToTrades = Map[String, HasVal[Trade]]()
  var tickerToQuotes = Map[String, HasVal[Quote]]()

  def writeToFile(url:URL, file:File) {
    logger.info(s"Downloading $url to $file")
    file.getParentFile.mkdirs()
    val writer = new BufferedWriter(new FileWriter(file))
    for (line <- Source.fromURL(url).getLines()) {
      writer.write(line)
      writer.write("\n")
    }
    writer.close()
  }

  def getTradeEvents(name:String):HasVal[Trade] = {
    val stream = tickerToTrades.get(name)
    if (stream.isDefined) return stream.get
    val file = new File(dataRoot, s"trades/$name")
    if (!file.exists()) {
      writeToFile(new URL(s"http://hopey.netfonds.no/tradedump.php?date=$date&paper=$name"), file)
    } else {
      logger.info(s"using cached file from $file")
    }
    val dateFormat = new SimpleDateFormat("yyyyMMdd'T'hhmmss")
    val source = Source.fromFile(file).getLines()
    val head = source.next()
    val data:Iterator[Trade] = source.map { line =>
      line.split('\t') match { case Array(time, price, size, _*)
        => new Trade(dateFormat.parse(time).getTime, price.toDouble, size.toLong, "", "", "", "", "")}
    }

    val trades = IteratorEvents(data)((t,i) => t.time)
    tickerToTrades += name -> trades
    trades
  }

  def getQuoteEvents(name:String):HasVal[Quote] = {
    val stream = tickerToQuotes.get(name)
    if (stream.isDefined) return stream.get
    val file = new File(dataRoot, s"quotes/$name")
    if (!file.exists()) {
      writeToFile(new URL(s"http://hopey.netfonds.no/posdump.php?date=$date&paper=$name"), file)
    } else {
      logger.info(s"using cached file from $file")
    }
    val dateFormat = new SimpleDateFormat("yyyyMMdd'T'hhmmss")
    val source = Source.fromFile(file).getLines()
    val head = source.next()
    val data:Iterator[Quote] = source.map( line => {
      line.split('\t') match {case Array(time, bid, bidDepth, bd2, ask, askDepth, ad2)
        => new Quote(dateFormat.parse(time).getTime, bid.toDouble, bidDepth.toLong, ask.toDouble, askDepth.toLong)}
    })
    val quotes = IteratorEvents(data)((t,i) => t.time)
    tickerToQuotes += name -> quotes
    quotes
  }
}

object TestTrade extends RealTradeTests {
  val universe = impl.asVector(List("MSFT.O"))
  val trades = universe.derive(u => getTradeEvents(u))
  Plot.plot(trades.map(_.price))
  impl.run(100000)
}

object TestPlots extends RealTradeTests {
  val trades = impl.asStream( getTradeEvents("MSFT.O") )
  val quotes = impl.asStream( getQuoteEvents("MSFT.O") )
  //  val accvol = trades.map(_.quantity).fold_all(new Sum[Long])
  Plot.plot(quotes.map(_.bid), "Bid").plot(quotes.map(_.ask), "Ask").plot(trades.map(_.price), "Trade")
  impl.run(10000)
}

object TestReduce extends RealTradeTests {
  val universe = impl.asVector(List("MSFT.O", "AAPL.O", "IBM.N"))
//  val universe = impl.asVector(List("MSFT.O"))
  val trades = universe.derive(u => getTradeEvents(u))
  val quotes = universe.derive(u => getQuoteEvents(u))

  class Red(key:String) extends Bucket {
    def value: Red = this

    var t:Trade = _
    var q:Quote = _
    var events:Int = 0

    def addTrade(t:Trade) {
      this.t = t
    }
    def addQuote(q:Quote) {
      this.q = q
    }


    def calculate(): Boolean = {
      events += 1
      true
    }

    override def toString: String = s"trade:$t, quote:$q, events:$events"
  }
  import scala.concurrent.duration._
  val oneMinute = new Timer(1 minute)
  val summary = trades.deriveSliced(new Red(_)).reduce().join(trades)(_.addTrade).join(quotes)(_.addQuote).slice_post(oneMinute)
  out("Summary")(summary)
//  Plot.plot(summary.map(_.q.ask))
  impl.run(50000)
}

object TestBucket extends RealTradeTests {
  val universe = impl.asVector(List("MSFT.O"))
//  val universe = impl.asVector(List("MSFT.O"))
  val trades = universe.derive(u => getTradeEvents(u))
  val quotes = universe.derive(u => getQuoteEvents(u))

  class Red(key:String) extends Bucket with types.MFunc {
    var t:Trade = _
    var q:Quote = _
    var events:Int = 0

    val tradeStream = trades(key).input
    env.addListener(tradeStream.getTrigger, this)

    val quoteStream = quotes(key).input
    env.addListener(quoteStream.getTrigger, this)

    def calculate(): Boolean = {
      events += 1
      if (env.hasChanged(tradeStream.getTrigger)) {
        t = tradeStream.value
      }
      if (env.hasChanged(quoteStream.getTrigger)) {
        q = quoteStream.value
      }
      true
    }


    // should I try to unify event and calculate?
    def event(): Boolean = {
      println("event on bucket")
      true
    }

    def value: Int = events
  }
  import scala.concurrent.duration._
  val oneMinute = new Timer(1 minute)
  val summary = universe.deriveSliced(new Red(_)) reduce() slice_post(oneMinute)
  Plot.plot(summary.map(_.events))
  impl.run(10000)
}

object TradeCategories extends RealTradeTests {
//  val universe = impl.asVector(List("MSFT.O", "AAPL.O", "IBM.N"))
  val universe = impl.asVector(List("MSFT.O"))
  val trades = universe.derive(u => getTradeEvents(u))
  val quote = universe.derive(u => getQuoteEvents(u))
  val mids = quote.map(q => (q.bid + q.ask) * 0.5)

  def categorise(p:(Trade, Quote)):(String, Trade) = {
    p match {
    case (trade, mid) if (trade.price >= mid.ask) => ("BUY", trade):(String,Trade)
    case (trade, mid) if (trade.price <= mid.bid) => ("SELL", trade):(String,Trade)
    case (trade,_) => ("NONE", trade):(String,Trade)
    }}


  val tradeCategory = trades.take(quote, identity).map(categorise)
  // crude impl before I have VectTerm.by
  val buys = tradeCategory.filter(_._1 == "BUY")
  val sells = tradeCategory.filter(_._1 == "SELL")
  val mid = tradeCategory.filter(_._1 == "NONE")

  val buyPressure = buys.fold_all(new Counter).map(_.c)
  val sellPressure = sells.fold_all(new Counter).map(_.c)
//  val midCountbuckets = mid.reduce(new Counter).slice_post(Timer 5 Minutes)
  val midCount = mid.fold_all(new Counter).map(_.c)

//  val buyPressure = buys.map(_._2.quantity).fold_all(new Sum[Long]).map(_.sum)
//  val sellPressure = sells.map(_._2.quantity).fold_all(new Sum[Long]).map(_.sum)
 val buySellDiff = buyPressure.join(sellPressure).map(e => e._1 - e._2)
  Plot.plot(buySellDiff)
//  Plot.plot(buys.map(_._2.price)).plot(sells.map(_._2.price)).plot(mid.map(_._2.price))
//  Plot.plot(mid.map(_._2.price)).plot(mids)
//  out("cat")(tradeCategory)
  impl.run(100000)
}

