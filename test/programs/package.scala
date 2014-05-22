import scala.math
import scala.util.Random
import scespet.core._
import scespet.core.types

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 04/04/2013
 * Time: 20:54
 * To change this template use File | Settings | File Templates.
 */
package object programs {
  case class SimpleTrade(name:String, price:Double, qty:Double)

  abstract class EventGenerator[X](nextTime:Long) extends EventSourceX[X] {
    var hasNext = true
    var getNextTime = nextTime
    var value:X = _

    def trigger:scespet.core.types.EventGraphObject = this


    def generate(): (X, Long)

    def advanceState() {
      initialised = true
      var xAndNext = generate()
      value = xAndNext._1
      if (xAndNext._2 == Long.MaxValue) {
        hasNext = false
      } else {
        getNextTime = xAndNext._2
      }
    }
  }

  def newRandom(env:types.Env) = new EventSourceX[Double] {
    var getNextTime: Long = env.getEventTime()
    val random = new Random()
    var value = random.nextDouble()
    initialised = true

    def trigger:scespet.core.types.EventGraphObject = this

    def hasNext(): Boolean = true

    def advanceState() {
      value = random.nextDouble()
      getNextTime += 1000
    }
  }

//  class BBO(val name:String, mid:HasVal[Double], spread:HasVal[Double])(implicit env:types.Env) extends IsVal(this) {
//    env.addListener()
//  }

  class PriceFactory(val env:types.Env) {
    val nameToMidStream = Map[String, HasVal[Double]]()
    val nameToTradeStream = Map[String, HasVal[SimpleTrade]]()

    def getMids(name:String) = {
      var midStreamO = nameToMidStream.get(name)
      midStreamO.getOrElse({
        val mids = newRandom(env)
        nameToMidStream + name -> mids
        mids
      })
    }

    def getTrades(name:String) = {
      var tradeStreamO = nameToTradeStream.get(name)
      tradeStreamO.getOrElse({
        val rand = new Random()
        val mids =  getMids(name)
        val trades = new EventGenerator[SimpleTrade](env.getEventTime) {
          override def generate(): (SimpleTrade, Long) = {
            val mid = mids.value
            val offset = (rand.nextDouble() - 0.5) / mid
            val trade = new SimpleTrade(name, mid + offset, rand.nextInt(1000))
            val nextTime = env.getEventTime + rand.nextInt(60000)
            (trade, nextTime)
          }
        }
        nameToTradeStream + name -> trades
        trades
      })
    }
  }
}
