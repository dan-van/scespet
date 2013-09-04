package programs

import java.util.logging.Level
import scespet.core._
import scespet.util._
import gsa.esg.mekon.run._
import gsa.esg.mekon.business.Region
import gsa.esg.mekon.run.TestConfig.Feeds
import gsa.esg.mekon.components.adapters.{FeedTradeSourceFactory, FeedSymbolMetaData, FeedDictionary, FeedRepository}

import gsa.esg.mekon.business.{BusinessDictionary, Region}
import scespet.EnvTermBuilder
import gsa.esg.mekon.run.TestConfig.Feeds
import gsa.esg.mekon.components.adapters.FeedRepository.FeedConfig
import scespet.expression.{AbsTerm, Scesspet}
import scala.collection.JavaConversions._
import gsa.esg.mekon.business.id.GSACode
import scala.collection.parallel.mutable
import gnu.trove.list.array.{TDoubleArrayList, TLongArrayList}
import gsa.esg.mekon.business.price.{Trade, RawBook}
import gsa.esg.mekon.components.trading.feeds.sirocco.SiroccoFeedFactory
import gsa.esg.mekon.components.Timer
import gsa.esg.mekon.components.trading.feeds.sirocco.SiroccoFeedFactory.Replay
import gsa.esg.mekon.components.trading.feeds.keyframe.KeyframeFeedFactory
import gsa.esg.mekon.components.trading.{Feed, FeedTradeSource}
import gsa.esg.mekon.components.trading.feeds.FeedSwitchStrategy
import gsa.esg.mekon.core.{EventGraphObject, Environment}
import gsa.esg.mekon.MekonRunner
import java.util.concurrent.TimeUnit
import java.util
import gsa.esg.mekon.util.CsvWriter


/**
 * @version $Id$
 */
//class TestMekon {
  // ---- executing term ----

  object Test extends App {
//    // termCollector should know the type of its root nodes.
//    // this allows specific builders to know if they can build from a given collector
//    java.util.logging.Logger.getLogger("gsa.naming").setLevel(Level.SEVERE)
//    //    java.util.logging.Logger.getLogger("gsa.esg.mekon").setLevel(Level.WARNING)
//    var prog = new Scesspet()
//    //    var root = prog.trace("Hello")
//    //    var root = prog.query(Seq("Hello", "there", "dan"))
//    var root = prog.query(env => IteratorEvents[String](Seq("Hello", "there", "dan")))
//
//    var query = out("is > 3 chars: ") {root.map(_.length).filter(_ > 3)}
//
//    //    new SimpleEvaluator().run(prog)
//    //    var env = new SimpleEnv
//    //    val cfg = new Configurator()
//    val cfg = new Configurator()
//    cfg.setTimeRange("yesterday", Region.EU)
//    val run = cfg.newRunner()
//    //    var env = new MekonEnv
//    val env = run.getEnvironment
//    new EnvTermBuilder(env).query(query.asInstanceOf[AbsTerm[_,_]])
//    run.run()
//    //    var result = new SimpleEvaluator().run(query)
//    //    println("collected data = "+result)
//    //    new MekonEvaluator().run(prog)
//
  }

  object PrintFeedEntries extends App {
//    java.util.logging.Logger.getLogger("gsa.naming").setLevel(Level.SEVERE)
//    //    java.util.logging.Logger.getLogger("gsa.esg.mekon").setLevel(Level.WARNING)
//    //    new SimpleEvaluator().run(prog)
//    //    var env = new SimpleEnv
//    //    val cfg = new Configurator()
    val cfg = new TestConfig()
    cfg.setTimeRange("yesterday", Region.EU)
    cfg.addFeeds("ReutersTrade-EU", Feeds.KEYFRAME)
//    val run = cfg.newRunner()
//    //    var env = new MekonEnv
//    val env = run.getEnvironment
//
//    val feedConfigs = env.getService(classOf[FeedRepository]).getFeedConfigs.asScala.filter(_.getGroupName.contains("Reuters-EU"))
//    //    val feeds = feedConfigs.iterator().asScala.map(x => x.inflate(env))
//    //    val feedDicts = feeds.map(_.getFeedDictionary)
//    val vector = new MutableVector(feedConfigs.asJava, env)
//    val start = new VectTerm(env)(vector)
//
//    class Printer extends Reduce[AnyRef] {
//      def add(x: AnyRef) {
//        println("Added "+x+" entries")
//      }
//    }
//    //    start.map(_.inflate(env)).map(_.getFeedDictionary).map(_.getAdded).fold_all2(new Printer)
//    start.map(_.inflate(env).getFeedDictionary).map(_.getAdded.asScala.toSet).fold_all2(new Printer)
//
//    //    new EnvTermBuilder(env).query(query.asInstanceOf[AbsTerm[_,_]])
//    run.run()

  }

  object PrintTrades extends App {
//    java.util.logging.Logger.getLogger("gsa.naming").setLevel(Level.SEVERE)
//    //    java.util.logging.Logger.getLogger("gsa.esg.mekon").setLevel(Level.WARNING)
//    //    new SimpleEvaluator().run(prog)
//    //    var env = new SimpleEnv
//    //    val cfg = new Configurator()
    val cfg = new TestConfig()
    cfg.setTimeRange("2013-09-02", Region.WORLD)
//    cfg.install(new SiroccoFeedFactory.Live().setFullPacketRetransEnabled(true))
//    cfg.install(new KeyframeFeedFactory)
//    cfg.install(new Replay())
    cfg.addFeeds("SiroccoReuters-MISC", Feeds.SIROCCO)
//    cfg.setProperties("FeedFactory.feedFilter=SiroccoReuters-AP")

    val run = cfg.newRunner()
    val env = run.getEnvironment


    run.runTo("07:30")
    val dict = env.getService(classOf[BusinessDictionary])

    val builder = new EnvTermBuilder(env)
    val trades = builder.query(env.getService(classOf[CoreServices]).acquireTradeSource(new GSACode("VOD.L")))
//    val trades = builder.queryE(env.getService(classOf[FeedTradeSourceFactory]).getTradeSource(dict.lookup( new GSACode("VOD.L") ), FeedSwitchStrategy.MAX_LIQUIDITY_NO_COMPOSITE))
    val tradesPerMinute = trades map(_.getNewTrade) filter(_ != null) reduce(new Counter2) reset_post( Timer.getMidnightAnchored(60000, 0, env) )
    val prettyOut = tradesPerMinute.map(x => {"at "+env.prettyPrintClockTime()+" perMinute: "+x.c})
    out( s"Trades per minute ").apply(prettyOut)
    out( s"Trade: " ) {
      var map: MacroTerm[Trade] = trades.map(_.getNewTrade)
      map.filter(_.getPrice().doubleValue() > 100)
    }
//    val accvol = trades.map(_.getNewTrade).filter(_ != null).map(_.getAmount.intValue()).reduce(new Sum).all()
    run.runUntilFired(trades.input.getTrigger)
    println(s"time is ${env.prettyPrintClockTime()}, and trade is ${trades.input.value.getNewTrade}")
    run.runTo("10:00")
//    // todo: need to trigger a bucket close on system shutdown
//    println("Final ACCVOL = "+accvol.input.asInstanceOf[BucketMaintainer[Sum, _]].nextBucket)
  }

object PrintStale extends App {
  //    java.util.logging.Logger.getLogger("gsa.naming").setLevel(Level.SEVERE)
  //    //    java.util.logging.Logger.getLogger("gsa.esg.mekon").setLevel(Level.WARNING)
  //    //    new SimpleEvaluator().run(prog)
  //    //    var env = new SimpleEnv
  //    //    val cfg = new Configurator()
  val cfg = new TestConfig()
//  cfg.setTimeRange("today", Region.AP)
  cfg.setTimeRange("2013-06-28", Region.AP)
//  cfg.install(new SiroccoFeedFactory.Live().setFullPacketRetransEnabled(true))
  cfg.install(new Replay())
//  cfg.setProperties("FeedFactory.feedFilter=SiroccoReuters-AP")

  val run = cfg.newRunner()
  val env = run.getEnvironment


  val builder = new EnvTermBuilder(env)
  val apConfigs = env.getService(classOf[FeedRepository]).getFeedConfigs.filter(_.getGroupName == "SiroccoReuters-AP")
  new MutableVector[FeedDictionary[_ <: FeedSymbolMetaData]](apConfigs.map(_.inflate(env).getFeedDictionary), env)
  val trades = builder.query(env.getService(classOf[CoreServices]).acquireTradeSource(new GSACode("4768.T"))).map(_.getNewTrade).filter( _ != null)
  out(""){trades}
//  val tradesPerMinute = trades reduce(new Counter2) reset_post( Timer.getMidnightAnchored(60000, 0, env) )
//  out( s"Trades per minute " ){tradesPerMinute}
  //    val accvol = trades.map(_.getNewTrade).filter(_ != null).map(_.getAmount.intValue()).reduce(new Sum).all()
  run.run()
  //    // todo: need to trigger a bucket close on system shutdown
  //    println("Final ACCVOL = "+accvol.input.asInstanceOf[BucketMaintainer[Sum, _]].nextBucket)
}

object RealtimePrint extends App {
  import EnvTermBuilder.eventObjectToHasVal
  val cfg = new TestConfig()
  cfg.setTimeRange("today", Region.EU)
  cfg.addFeeds("SiroccoReuters-EU", Feeds.SIROCCO)
  cfg.install(new MekonProgram[Unit] {
    def initProgram(env: Environment) {
      val builder = new EnvTermBuilder(env)
      val trades = builder.query( acquireTradeSource(new GSACode("VOD.L")) )
      val accVol = trades.filter(_.getNewTrade != null).map(_.getNewTrade.getAmnt).fold_all(new Sum[Long])
      out( s"Trade at ${env.prettyPrintClockTime()}:" ){ trades.map(_.getNewTrade) }
      out(s"AccVol at ${env.prettyPrintClockTime()}:") {accVol}
    }
  })
  cfg.run
}

object RawTrades extends App {
  val cfg = new TestConfig
  cfg.setTimeRangeBusinessDate("2013-09-02", "ATLFUT")
  cfg.install(new SiroccoFeedFactory.Replay(), "SiroccoFeed.promiscuousReplay=false")

  val run = cfg.newRunner()
  val env = run.getEnvironment
  val builder = new EnvTermBuilder(env)

  val configs = env.getService(classOf[SiroccoFeedFactory]).getFeedConfigs.filter(_.getGroupName == "SiroccoReuters-MISC")
  println(s"got ${configs.size} feed configs")

  var dictionaries = builder.asVector(configs.map(_.inflate(env).getFeedDictionary))
  // add a printer for all added entries to all dictionaries:
//  dictionaries.map(d => {println(s"${d.getFeed.getName} added entries: ${d.getAdded}")})

  // take a subset of keys that match a predicate
  val allFeedKeys = dictionaries.valueSet[FeedSymbolMetaData](_.getAdded)
  val eventsByKey = allFeedKeys.joinf(x => x.getFeed.asInstanceOf[Feed[EventGraphObject, FeedSymbolMetaData]].acquireEventSourceReference(x))
  var eventCounts = eventsByKey.fold_all(new Counter)

  run.run()

  println("event counts: ")
  val size: Int = eventCounts.input.getSize
  println(s"\nAt ${env.prettyPrintClockTime()} we have $size feed keys:")
  val writer = new CsvWriter("EventCounts.csv")
  writer.writeRecord(Array("symbol", "type", "count"))
  for (i <- 0 to size - 1) {
    val metaData: FeedSymbolMetaData = eventCounts.input.getKey(i)
    println(s"\t \t ${metaData.getFeed.getEventSourceType.getSimpleName} : ${metaData.getKey} has ${eventCounts.input.get(i).c} events")
    writer.writeRecord(Array(metaData.getKey, metaData.getFeed.getEventSourceType.getSimpleName, String.valueOf(eventCounts.input.get(i).c)))
  }
  writer.close()
}

//}
