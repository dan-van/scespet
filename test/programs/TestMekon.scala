package programs

import java.util.logging.Level
import scespet.core._
import scespet.util._
import gsa.esg.mekon.run.TestConfig
import gsa.esg.mekon.business.Region
import gsa.esg.mekon.run.TestConfig.Feeds
import gsa.esg.mekon.components.adapters.FeedRepository

import gsa.esg.mekon.run.{CoreServices, TestConfig, Configurator}
import gsa.esg.mekon.business.{BusinessDictionary, Region}
import scespet.EnvTermBuilder
import gsa.esg.mekon.run.TestConfig.Feeds
import gsa.esg.mekon.components.adapters.FeedRepository
import gsa.esg.mekon.components.adapters.FeedRepository.FeedConfig
import scespet.expression.{AbsTerm, Scesspet}
import scala.collection.JavaConverters._
import gsa.esg.mekon.business.id.GSACode
import scala.collection.parallel.mutable
import gnu.trove.list.array.{TDoubleArrayList, TLongArrayList}
import gsa.esg.mekon.business.price.RawBook
import gsa.esg.mekon.components.trading.feeds.sirocco.SiroccoFeedFactory
import gsa.esg.mekon.components.Timer
import gsa.esg.mekon.components.trading.feeds.sirocco.SiroccoFeedFactory.Replay


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
//    val cfg = new TestConfig()
//    cfg.setTimeRange("yesterday", Region.EU)
//    cfg.addFeeds("ReutersTrade-EU", Feeds.KEYFRAME)
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
    cfg.setTimeRange("today", Region.AP)
    cfg.install(new SiroccoFeedFactory.Live().setFullPacketRetransEnabled(true))
    cfg.install(new Replay())
    cfg.setProperties("FeedFactory.feedFilter=SiroccoReuters-AP")

    val run = cfg.newRunner()
    val env = run.getEnvironment


    val builder = new EnvTermBuilder(env)
    val trades = builder.queryE(env.getService(classOf[CoreServices]).acquireTradeSource(new GSACode("4768.T")))
    val tradesPerMinute = trades map(_.getNewTrade) filter(_ != null) reduce(new Counter2) reset_post( Timer.getMidnightAnchored(60000, 0, env) )
    out( s"Trades per minute " ){tradesPerMinute}
//    val accvol = trades.map(_.getNewTrade).filter(_ != null).map(_.getAmount.intValue()).reduce(new Sum).all()
    run.run()
//    // todo: need to trigger a bucket close on system shutdown
//    println("Final ACCVOL = "+accvol.input.asInstanceOf[BucketMaintainer[Sum, _]].nextBucket)
  }

//}
