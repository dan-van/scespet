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
import gsa.esg.mekon.business.price.RawBook
import gsa.esg.mekon.components.trading.feeds.sirocco.SiroccoFeedFactory
import gsa.esg.mekon.components.Timer
import gsa.esg.mekon.components.trading.feeds.sirocco.SiroccoFeedFactory.Replay
import gsa.esg.mekon.components.trading.feeds.keyframe.KeyframeFeedFactory
import gsa.esg.mekon.components.trading.{Feed, FeedTradeSource}
import gsa.esg.mekon.components.trading.feeds.FeedSwitchStrategy
import gsa.esg.mekon.core.Environment
import gsa.esg.mekon.MekonRunner


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
    cfg.setTimeRange("2013-07-10", Region.EU)
//    cfg.install(new SiroccoFeedFactory.Live().setFullPacketRetransEnabled(true))
//    cfg.install(new KeyframeFeedFactory)
//    cfg.install(new Replay())
    cfg.addFeeds("*", Feeds.KEYFRAME)
//    cfg.setProperties("FeedFactory.feedFilter=SiroccoReuters-AP")

    val run = cfg.newRunner()
    val env = run.getEnvironment


    run.runTo("07:30")
    val dict = env.getService(classOf[BusinessDictionary])

    val builder = new EnvTermBuilder(env)
    val trades = builder.queryE(env.getService(classOf[CoreServices]).acquireTradeSource(new GSACode("VOD.L")))
//    val trades = builder.queryE(env.getService(classOf[FeedTradeSourceFactory]).getTradeSource(dict.lookup( new GSACode("VOD.L") ), FeedSwitchStrategy.MAX_LIQUIDITY_NO_COMPOSITE))
    val tradesPerMinute = trades map(_.getNewTrade) filter(_ != null) reduce(new Counter2) reset_post( Timer.getMidnightAnchored(60000, 0, env) )
    val prettyOut = tradesPerMinute.map(x => {"at "+env.prettyPrintClockTime()+" perMinute: "+x.c})
    out( s"Trades per minute " ){prettyOut}
    out( s"Trade: " ){trades.map(_.getNewTrade)}
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
  val trades = builder.queryE(env.getService(classOf[CoreServices]).acquireTradeSource(new GSACode("4768.T"))).map(_.getNewTrade).filter( _ != null)
  out(""){trades}
//  val tradesPerMinute = trades reduce(new Counter2) reset_post( Timer.getMidnightAnchored(60000, 0, env) )
//  out( s"Trades per minute " ){tradesPerMinute}
  //    val accvol = trades.map(_.getNewTrade).filter(_ != null).map(_.getAmount.intValue()).reduce(new Sum).all()
  run.run()
  //    // todo: need to trigger a bucket close on system shutdown
  //    println("Final ACCVOL = "+accvol.input.asInstanceOf[BucketMaintainer[Sum, _]].nextBucket)
}

object RealtimePrint extends App {
  val cfg = new TestConfig()
  cfg.setTimeRange("today", Region.EU)
  cfg.addFeeds("SiroccoReuters-EU", Feeds.SIROCCO)
  cfg.install(new MekonProgram[Unit] {
    def initProgram(env: Environment) {
      val builder = new EnvTermBuilder(env)
      val trades = builder.queryE( acquireTradeSource(new GSACode("VOD.L")) )
      val accVol = trades.filter(_.getNewTrade != null).map(_.getNewTrade.getAmnt).fold_all(new SumN[Long])
      out( s"Trade at ${env.prettyPrintClockTime()}:" ){ trades.map(_.getNewTrade) }
      out(s"AccVol at ${env.prettyPrintClockTime()}:") {accVol}
    }
  })
  cfg.run
}

object RawTrades extends App {
  val cfg = new TestConfig
  cfg.setTimeRangeBusinessDate("2013-08-22", "EU")
  cfg.install(new SiroccoFeedFactory.Replay(), "SiroccoFeed.promiscuousReplay=false")

  val run = cfg.newRunner()
  val env = run.getEnvironment
  val builder = new EnvTermBuilder(env)
  val tradeFeedDicts = env.getService(classOf[SiroccoFeedFactory]).getFeedConfigs.filter(_.getGroupName == "SiroccoReuters-EU").filter(c => classOf[FeedTradeSource].isAssignableFrom(c.getFeedObjectType)).map(_.inflate(env).getFeedDictionary)
  println("got "+tradeFeedDicts.size)

  var dictionaries = builder.query(tradeFeedDicts)
  dictionaries.map(d => {println(s"${d.getFeed.getName} added entries: ${d.getAdded}")})
  private var interstingKeys :VectTerm[FeedSymbolMetaData, FeedSymbolMetaData] = dictionaries.valueSet(_.getAdded.filter(_.getKey == "ADENE.S"))
  out("keys"){interstingKeys}
  private var tradeSource: VectTerm[FeedSymbolMetaData, FeedTradeSource] = interstingKeys.joinf(_ match {case (k:FeedSymbolMetaData) => {
    new IsVal( k.getFeed.asInstanceOf[Feed[FeedTradeSource, FeedSymbolMetaData]].acquireEventSourceReference(k) )
  }})
  var tradeCount = tradeSource.fold_all2(new Counter2)
  out("tradeCount"){
    tradeCount }
  run.runTo("09:00")
  println(interstingKeys.input.getKeys)
}

//}
