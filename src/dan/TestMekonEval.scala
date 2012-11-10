import collection.mutable.ArrayBuffer
import gsa.esg.mekon.business.id.RIC
import gsa.esg.mekon.business.price.{Side, Book, RawBook}
import gsa.esg.mekon.business.{TradingSession, Region}
import gsa.esg.mekon.components.stats.{DelayedData, UnivariateStats}
import gsa.esg.mekon.components.TimeWindowMovingAverage
import gsa.esg.mekon.components.trading.FeedTradeSource
import gsa.esg.mekon.core.Environment
import gsa.esg.mekon.MekonRunner
import gsa.esg.mekon.run.TestConfig.Feeds
import gsa.esg.mekon.run.{CoreServices, TestConfig, Configurator}
import scespet.core._


val config = new TestConfig()
config.setTimeRange("yesterday", Region.EU)
config.addFeeds("Reuters-EU", Feeds.KEYFRAME)



val run: MekonRunner = config.newRunner()
val env: Environment = run.getEnvironment
val core: CoreServices = env.getService(classOf[CoreServices])
val eval = new MekonEval(env)

val bbo: RawBook = core.acquireBBO(new RIC("VOD.L"))
val tradeStream = eval.stream(core.acquireTradeSource(new RIC("VOD.L")))
        .filter(_.getNewTrade != null)
        .map(_.getNewTrade)


//tradeStream.filter(_.getPrice > )map(t => {val mid = Book.Util.getMid(bbo); println(s"mid = $mid and trade = $t")})
tradeStream.filter(_.getPrice.doubleValue() > Book.Util.getMid(bbo)).map(x => println(s"Buy: $x"))
tradeStream.filter(_.getPrice.doubleValue() < Book.Util.getMid(bbo)).map(x => println(s"Sell: $x"))


class CumuSum extends AbsFunc[Double, Double]{
  var stats = new UnivariateStats(100)
  def calculate() = {value = value + source.value; stats.addData(source.value);true}
}

//val scaledPrices = tradeStream
//        .map(x => {println(s"Got trade: $x"); x.getPrice})
//        .map(_.doubleValue() * 100.0)
//        .map(x => {println(s"Scaled: $x"); x})


//val cumu: CumuSum = new CumuSum()

//val accVol: Expr[Double] = tradeStream.map(_.getAmnt.toDouble)
//        .map(cumu)
//
//accVol.map(x => println(s"AccVol: $x"))

//tradeStream.sel(new Select{var cash = in.getPrice.doubleValue() * in.getAmnt; var quantity = in.qty}).

run.run()


