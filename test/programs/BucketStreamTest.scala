package programs

import org.junit.runner.RunWith
import org.scalatest.junit.{ShouldMatchersForJUnit, AssertionsForJUnit, JUnitRunner}
import org.scalatest.{OneInstancePerTest, BeforeAndAfterEach, FunSuite}
import scespet.core._
import scespet.core.types.{MFunc, IntToEvents}
import scespet.util.{ScespetTestBase, Sum}
import scespet.EnvTermBuilder

/**
 * Created by danvan on 17/04/2014.
 */
@RunWith(classOf[JUnitRunner])
class BucketStreamTest extends ScespetTestBase with BeforeAndAfterEach with OneInstancePerTest with AssertionsForJUnit with ShouldMatchersForJUnit {
  
  var env:SimpleEnv = _
  var impl:EnvTermBuilder = _
  var data: Array[Char] = _
  var stream: MacroTerm[Char] = _

  override protected def beforeEach() {
    env = new SimpleEnv
    impl = EnvTermBuilder(env)
    data = "abcdefghijk".toCharArray
    stream = impl.asStream(IteratorEvents(data)((char, i) => i))
  }
  override protected def afterEach() {
    env.run()
//    for (r <- postRunChecks) {
//      r()
//    }
  }

  class Append[X] extends Reducer[X, Seq[X]] {
    var value: Seq[X] = Seq[X]()
    override def add(x: X): Unit = value :+= x
  }
  
  class AppendFunc[X] extends Bucket {
    type OUT = Seq[X]
    var value = Seq[X]()

    def add(x:X) {
      value :+= x
    }
    override def calculate(): Boolean = {
      true
    } 
  }

  def generateAppendScan(dat:Seq[Char]):Seq[Seq[Char]] = {
    dat.scanLeft(Seq[Char]())(_ :+ _).tail
  }

  test("scan") {
    val out = stream.scan(new Append[Char])
    val expected = generateAppendScan(data)
    new StreamTest("scan", expected, out)
  }

  test("reduce") {
    val out = stream.reduce(new Append[Char])
    val expected = List(data.foldLeft(Seq[Char]())(_ :+ _))
    new StreamTest("scan", expected, out)
  }

  test("grouped scan") {
    val out = stream.group(3.events).scan(new Append[Char])
    val expected = data.grouped(3).map( generateAppendScan(_) ).reduce( _ ++ _ )
    new StreamTest("scan", expected, out)
  }

  test("grouped reduce") {
    val out = stream.group(3.events).reduce(new Append[Char])
    val expected = data.grouped(3).map( generateAppendScan(_).last ).toSeq
    new StreamTest("scan", expected, out)
  }

  // -------- the same tests with a HasVal with binds instead of A Reducer

  test("bind scan") {
    val out = impl.streamOf2(new AppendFunc[Char]).bind(stream.input)(_.add).all()
    val expected = generateAppendScan(data)
    new StreamTest("scan", expected, out)
  }
//
//  test("bind fold") {
//    val out = stream.reduce2(new Append[Char]).last()
//    val expected = List(data.foldLeft(Seq[Char]())(_ :+ _))
//    new StreamTest("scan", expected, out)
//  }
//
//  test("bind grouped scan") {
//    val out = stream.reduce2(new Append[Char]).every(3.events).all
//    val expected = data.grouped(3).map( generateAppendScan(_) ).reduce( _ ++ _ )
//    new StreamTest("scan", expected, out)
//  }
//
//  test("bind grouped fold") {
//    val out = stream.reduce2(new Append[Char]).every(3.events).last
//    val expected = data.grouped(3).map( generateAppendScan(_).last ).toSeq
//    new StreamTest("scan", expected, out)
//  }

  //
//    val mult10 = streamOf(new Append[Char]).bind(_.add)(stream)
//    val mult10 = streamOf(new Append[Char]).bind(_.add)(stream).every(3.events)
//    val mult10 = streamOf(new Append[Char]).bind(_.add)(stream).last
//    val mult10 = streamOf(new Append[Char]).bind(_.add)(stream).last.every(3.events)
}
