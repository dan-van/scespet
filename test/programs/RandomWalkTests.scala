package programs

/**
 * Created by danvan on 01/08/2014.
 */
package randomWalkTests {

import data.Plot
import scespet.EnvTermBuilder
import scespet.core.SimpleEnv
import scespet.util.Sum

import scala.util.Random

object Demo extends App {
  implicit val env = new SimpleEnv
  val impl = EnvTermBuilder(env)

  val randomSource = impl.lazyVect((k:String) => new EventGenerator[Double](0) {
    val rand = new Random(k.hashCode)
    override def generate(): (Double, Long) = (rand.nextDouble()-0.5, getNextTime + 100)
  })

  randomSource("BAR.L")
  val walk = randomSource.scan(new Sum[Double])
  Plot.plot(walk)

  randomSource("FOO.L")

  env.run(500 * 100)
  Plot.waitForClose()
}

}
