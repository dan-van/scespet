package programs

import data.Plot
import scespet.core._
import java.io.{BufferedWriter, FileWriter, File}
import java.net.URL
import java.text.SimpleDateFormat
import scespet.util.{_}
import scespet.EnvTermBuilder
import scala.collection.mutable

//import org.msgpack._
//import org.msgpack.annotation.Message
//import org.msgpack.ScalaMessagePack._
import org.scalatest.time.Minutes
import scala.concurrent.duration._

// <- import MessagePack instance for scala

import scala.io.Source

case class Trade(time:Long,	price:Double,	quantity:Long,	board:String,	source:String,	buyer:String,	seller:String,	initiator:String)
case class Quote(time:Long,	bid:Double,	bidDepth:Long, ask:Double, askDepth:Long)

/**
 * This uses real trading data acquired on demmand (then cached) from http://hopey.netfonds.no/tradedump.php?date=
 */
abstract class RealTradeTests extends App with Logged {
  val date="20131029"
  val dataRoot = new File("./testdata")
  //time	price	quantity	board	source	buyer	seller	initiator

  //time	bid	bid_depth	bid_depth_total	offer	offer_depth	offer_depth_total
//  20131030T090001	515.1	300	300	518.9	300	300

  implicit val env = new SimpleEnv
  val impl = EnvTermBuilder(env)

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
  val trades = universe.keyToStream(u => getTradeEvents(u))
  Plot.plot(trades.map(_.price))
  env.run(100000)
}

object TestPlots extends RealTradeTests {
  val universe = impl.asVector(List("AAPL.O", "MSFT.O"))
//  val trades = impl.asStream( getTradeEvents("MSFT.O") )
  val trades = universe.keyToStream(name => getTradeEvents(name) )
//  val quotes = impl.asStream( getQuoteEvents("MSFT.O") )
  //  val accvol = trades.map(_.quantity).fold_all(new Sum[Long])
//  Plot.plot(quotes.map(_.bid), "Bid")
//  Plot.plot(quotes.map(_.ask), "Ask")
  val sizes = trades.map(_.quantity)
  val big = sizes.filter(_ > 25000)
  import scala.concurrent.duration._
  val s = sizes.red(new Sum[Long]).every(10.minutes)//(SliceTriggerSpec.toTriggerSpec( big ) )
//  Plot.plot(sizes).seriesNames(_ + "Size")
  Plot.plot(s).seriesNames(_ + "buckets")
  env.run(100000)
}

class TradeQuoteStats(key:String) extends Bucket {
  type OUT=TradeQuoteStats
  def value: TradeQuoteStats = this

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


object TestTradeQuoteJoin extends RealTradeTests {
  val trades = getTradeEvents("MSFT.O")
  val quotes = getQuoteEvents("MSFT.O")
//  impl.streamOf2(new TradeQuoteStats("MSFT.O")).bind(trades)(_.addTrade).bind(quotes)(_.addQuote).all()
//  impl.streamOf2(new TradeQuoteStats("MSFT.O")).bind(trades)(_.addTrade).bind(quotes)(_.addQuote).every(3.minutes)
}


object TestReduce extends RealTradeTests {
  val universe = impl.asVector(List("MSFT.O", "AAPL.O", "IBM.N"))
//  val universe = impl.asVector(List("MSFT.O"))
  val trades = universe.keyToStream(u => getTradeEvents(u))
  val quotes = universe.keyToStream(u => getQuoteEvents(u))

  import scala.concurrent.duration._
  val oneMinute = new Timer(1 minute)
//  val summary = trades.keyToStream(key => reduce(new TradeQuoteStats(_)).join(trades)(_.addTrade).join(quotes)(_.addQuote).every(oneMinute)
  // TODO: this seems pointless - we actually want a stream of buckets for the given keyset. How is this different form keyToStream?
  //NODEPLOY
  val summary = trades.keyToStream(k => impl.streamOf2(new TradeQuoteStats(k)).bind(trades(k))(_.addTrade).bind(quotes(k))(_.addQuote).every(oneMinute).last())
//  val summary = trades.reduce(new TradeQuoteStats(_)).bind(quotes)(_.addQuote).every(oneMinute).last()
//  out("Summary")(summary)
//  Plot.plot(summary.map(_.q.ask))
  env.run(50000)
}

object TestBucket extends RealTradeTests {
  val universe = impl.asVector(List("MSFT.O"))
//  val universe = impl.asVector(List("MSFT.O"))
  val trades = universe.keyToStream(u => getTradeEvents(u))
  val quotes = universe.keyToStream(u => getQuoteEvents(u))

  class Red(key:String) extends Bucket {
    type OUT=Int
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
  // hmmm, buildBucketStream is really not much different from a completely independent 'bucketStreamBuilder' function...
  val summary = universe.keyToStream(k => impl.streamOf2(new Red(k)).every(oneMinute).last())
  Plot.plot(summary)
  env.run(10000)
}

object TradeCategories extends RealTradeTests {
//  val universe = impl.asVector(List("MSFT.O", "AAPL.O", "IBM.N"))
  val universe = impl.asVector(List("MSFT.O"))
  val trades = universe.keyToStream(u => getTradeEvents(u))
  val quote = universe.keyToStream(u => getQuoteEvents(u))
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

  val buyPressure = buys.scan(new Counter).all()
  val sellPressure = sells.scan(new Counter).all()
//  val midCountbuckets = mid.reduce(new Counter).slice_post(Timer 5 Minutes)
  val midCount = mid.scan(new Counter).all()

//  val buyPressure = buys.map(_._2.quantity).fold_all(new Sum[Long]).map(_.sum)
//  val sellPressure = sells.map(_._2.quantity).fold_all(new Sum[Long]).map(_.sum)
 val buySellDiff = buyPressure.join(sellPressure).map(e => e._1 - e._2)
  Plot.plot(buySellDiff)
//  Plot.plot(buys.map(_._2.price)).plot(sells.map(_._2.price)).plot(mid.map(_._2.price))
//  Plot.plot(mid.map(_._2.price)).plot(mids)
//  out("cat")(tradeCategory)
  env.run(100000)
}

object SimpleSpreadStats extends RealTradeTests {
  val universe = impl.asVector(List("MSFT.O"))

  class SpreadStats(key:String) extends Bucket {
    type OUT=SpreadStats
    val value = this

    val quotes = getQuoteEvents(key)
    env.addListener(quotes.getTrigger, this)

    val spreadCounts = mutable.HashMap[Double, Int]()
    override def calculate(): Boolean = {
      val spread = quotes.value.ask - quotes.value.bid
      val now = spreadCounts.getOrElse(spread, 0)
      spreadCounts.put(spread, now + 1)
      true
    }

    def mode() = {
      spreadCounts.maxBy(e => e._2)._1
    }
  }
  import scespet.core.types._
  val spreadCounters = universe.keyToStream(k => impl.streamOf2(new SpreadStats(k)).every(10000.events).last()) //all()
  val modeSpread = spreadCounters.map(_.mode)
  out("ModeSpread")(modeSpread)
  env.run()
}

