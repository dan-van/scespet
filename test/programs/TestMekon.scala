package programs

import java.util.logging.Level
import scespet.core._
import scespet.util._
//import gsa.esg.mekon.run.{CoreServices, TestConfig, Configurator}
//import gsa.esg.mekon.business.{BusinessDictionary, Region}
//import scespet.EnvTermBuilder
//import gsa.esg.mekon.run.TestConfig.Feeds
//import gsa.esg.mekon.components.adapters.FeedRepository
//import gsa.esg.mekon.components.adapters.FeedRepository.FeedConfig
//import scespet.expression.{AbsTerm, Scesspet}
//import scala.collection.JavaConverters._
//import gsa.esg.mekon.business.id.GSACode
//import scala.collection.parallel.mutable
//import gnu.trove.list.array.{TDoubleArrayList, TLongArrayList}


/**
 * @version $Id$
 */
object TestMekon {
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
//    val vector = new MutableVector(classOf[FeedConfig], feedConfigs.asJava, env)
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
//
  }
}
