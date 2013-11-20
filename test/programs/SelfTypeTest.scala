package programs

/**
 * Created with IntelliJ IDEA.
 * User: danvan
 * Date: 01/11/2013
 * Time: 19:30
 * To change this template use File | Settings | File Templates.
 */
object SelfTypeTest extends App {
  // I want an interface that defines the 'combine' method, but specifies a type constraint for the minimal BuilderBase that the implementation requires as a parameter
  trait BuilderBase[X] {
    type MinB[X2] <: BuilderBase[X2]
    def combine[Y](other:MinB[Y]) : MinB[Y]
    def next() : MinB[X]
  }
  
  class BuildA[X] extends BuilderBase[X] {
    override type MinB[X2] = BuildA[X2]
    def combine[Y](other: BuildA[Y]): BuildA[Y] = ???
    def next(): BuildA[X] = ???
  }

  class BuildB[X] extends BuilderBase[X] {
    override type MinB[X] = BuildB[X]
    def combine[Y](other: BuildB[Y]): BuildB[Y] = ???
    def next(): BuildB[X] = ???
  }

//  class BuildA1[X] extends BuildA[X] {
//    override def combine[Y](other: BuildA[Y]): BuildA1[Y] = ???
//  }
  
  val buildA:BuildA[Int] = new BuildA[Int]().next()
  val buildB:BuildB[String] = new BuildB[String]().next()
}
