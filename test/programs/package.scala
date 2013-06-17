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
  def newRandom(env:types.Env) = new EventSourceX[Double] {
    var getNextTime: Long = env.getEventTime()
    val random = new Random()
    var value = random.nextDouble()

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
    val nameToBBO = Map[String, HasVal[Double]]()
    def getMids(name:String) = {
      var midStreamO = nameToMidStream.get(name)
      midStreamO.getOrElse({
        val mids = newRandom(env)
        nameToMidStream + name -> mids
        mids
      })
    }

    def getBBO(name:String) = {
      getMids(name)
    }
  }
}
