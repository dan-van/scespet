package programs

import org.junit.runner.RunWith
import org.scalatest.junit.{ShouldMatchersForJUnit, AssertionsForJUnit, JUnitRunner}
import org.scalatest.{OneInstancePerTest, BeforeAndAfterEach, FunSuite}
import scespet.core.CellOut2.Ident2
import scespet.core._
import scespet.core.types.{MFunc, IntToEvents}
import scespet.util.{ScespetTestBase, Sum}
import scespet.EnvTermBuilder

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
 * Created by danvan on 17/04/2014.
 */
@RunWith(classOf[JUnitRunner])
class BucketStreamTest extends ScespetTestBase with BeforeAndAfterEach with OneInstancePerTest with AssertionsForJUnit with ShouldMatchersForJUnit {
  
  var env:SimpleEnv = _
  var impl:EnvTermBuilder = _
  var data: Array[Char] = _
  var windowedData: Seq[Seq[Char]] = _
  var stream: MacroTerm[Char] = _
  var windowStates :Seq[Boolean] = _
  var inWindow :IndexedSeq[Int] = _
  var windowIndicies :Seq[Seq[Int]] = _

  override protected def beforeEach() {
    super.beforeEach()
    env = new SimpleEnv
    impl = EnvTermBuilder(env)
    data = "abcdefghijk".toCharArray
    stream = impl.asStream(IteratorEvents(data)((char, i) => i))
    windowIndicies = Seq((0 to 2), (5 to 8))
    inWindow = windowIndicies.flatten.toIndexedSeq
    // get the elements that have indicies in the inWindow definition
    windowedData = windowIndicies.map(idxs => idxs.map(i => data(i)))

    windowStates = (0 until data.length).map(x => inWindow.contains(x))
  }

  def buildWindowStream = {
    stream.map(s => {val t = env.getEventTime.toInt; inWindow.contains(t)})
  }

  override protected def afterEach() {
    env.run()
    super.afterEach()
  }

  class Append[X] extends Reducer[X, Seq[X]] {
    var value: Seq[X] = Seq[X]()
    override def add(x: X): Unit = value :+= x
  }

  class SumX[X:Numeric] extends Reducer[X, Double]{
    var sum = 0.0
    def value = sum
    def add(n:X):Unit = {sum = sum + implicitly[Numeric[X]].toDouble(n)}

    override def toString = s"Sum=$sum"
  }

  class AppendWithOutTrait[X] extends CellAdder[X] with OutTrait[Seq[X]] {
    var value: Seq[X] = Seq[X]()
    override def add(x: X): Unit = value :+= x
  }

  class OldStyleFuncAppend[X](in:HasVal[X], env:types.Env) extends Bucket with OutTrait[Seq[X]] {
    var value = Seq[X]()
    env.addListener(in.trigger, this)
    override def calculate(): Boolean = {
      append(in.value)
      true
    }

    def append(x: X) {
      value :+= x
    }

    override def open(): Unit = value = Nil
  }

  class BindableAppendFunc[X] extends Bucket with OutTrait[Seq[X]] {
    var value = Seq[X]()

    def add(x:X) {
      value :+= x
    }
    override def calculate(): Boolean = {
      true
    }

    override def open(): Unit = value = Nil
  }

  /**
   * given "abc", we return ("a","ab","abc")
   * @param dat
   * @return
   */
  def generateAppendScan(dat:Seq[Char]):Seq[Seq[Char]] = {
    dat.scanLeft(Seq[Char]())(_ :+ _).tail
  }

  test("scan") {
    val out = stream.scan(new Append[Char])
    val expected = generateAppendScan(data)
    new StreamTest("scan", expected, out)
  }

  test("scan non agg") {
    val testStream = impl.asStream(IteratorEvents("abab")((char, i) => i))

    implicit val adder = (set:mutable.HashSet[Char]) => new CellAdder[Char] {
      override def add(x: Char): Unit = set.add(x)
    }
    implicit val view = new AggOut[mutable.HashSet[Char], Set[Char]] {
      override def out(a: mutable.HashSet[Char]): Set[Char] = a.toSet
    }
    val out = testStream.scan(new collection.mutable.HashSet[Char])
    val expected = List(
      Set('a'),
      Set('a','b'),
      Set('a','b'),
      Set('a','b')
    )
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

  test("grouped scanI2") {
    val out = stream.group(3.events).scan(new AppendWithOutTrait[Char])
    val expected = data.grouped(3).map( generateAppendScan(_) ).reduce( _ ++ _ )
    new StreamTest("scan", expected, out)
  }

  test("grouped scanI3 a") {
    class MyAdder extends CellAdder[Char] {
      var myOut:Char = _
      override def add(x: Char): Unit = myOut = x
    }

    implicit def anyToIdent[A] = new AggOut[A,A] {
      override def out(a: A): A = a
    }

    val out = stream.group(3.events).scan(new MyAdder)
    val expected = data.toSeq
    new StreamTest("scan", expected, out.map(_.myOut))
  }

  test("grouped scanI3 b") {
//    val out = stream.group(3.events).scanI3(new AppendWithOutTrait[Char])
//    val expected = data.grouped(3).map( generateAppendScan(_) ).reduce( _ ++ _ )
//    new StreamTest("scan", expected, out)
  }

  test("grouped reduce") {
    val out = stream.group(3.events).reduce(new Append[Char])
    val expected = data.grouped(3).map( generateAppendScan(_).last ).toSeq
    new StreamTest("scan", expected, out)
  }

  test("windowDefinition"){
    val windowStream = buildWindowStream
    new StreamTest("window", windowStates, windowStream)
  }

  test("window scan") {
    val out = stream.window( buildWindowStream ).scan(new Append[Char])
    var expected = windowedData.map( generateAppendScan(_) ).flatten
    new StreamTest("scan", expected, out)
  }

  test("windowed reduce") {
    // NODEPLOY - need to further define and test semantics of windows not closing before termination.
    // current test has the second window close before end.
    val out = stream.window( buildWindowStream ).reduce(new Append[Char])
    var expected = windowedData.toSeq
    new StreamTest("scan", expected, out)
  }

  // -------- the same tests with a HasVal with binds instead of A Reducer

  test("bind scan") {
    val out = stream.bindTo(new BindableAppendFunc[Char])(_.add).all()
    val expected = generateAppendScan(data)
    new StreamTest("scan", expected, out)
  }

  test("bind reduce") {
    val out = stream.bindTo(new BindableAppendFunc[Char])(_.add).last()
    val expected = List(data.foldLeft(Seq[Char]())(_ :+ _))
    new StreamTest("scan", expected, out)
  }

  test("bind grouped scan") {
    val out = stream.group(3.events).collapseWith2(new BindableAppendFunc[Char])(_.add).all()
    val expected = data.grouped(3).map( generateAppendScan(_) ).reduce( _ ++ _ )
    new StreamTest("scan", expected, out)
  }

  test("bind grouped reduce") {
    val out = stream.group(3.events).collapseWith2(new BindableAppendFunc[Char])(_.add).last()
    val expected = data.grouped(3).map( generateAppendScan(_).last ).toSeq
    new StreamTest("scan", expected, out)
  }

  // -------------- Pure old-style function streams
  test("MFunc scan") {
    val out = impl.streamOf2(new OldStyleFuncAppend[Char](stream.input, env)).all()
    val expected = generateAppendScan(data)
    new StreamTest("scan", expected, out)
  }

  test("MFunc reduce") {
    val out = impl.streamOf2(new OldStyleFuncAppend[Char](stream.input, env)).last()
    val expected = List(data.foldLeft(Seq[Char]())(_ :+ _))
    new StreamTest("scan", expected, out)
  }

  test("MFunc grouped scan") {
    val out = impl.streamOf2(new OldStyleFuncAppend[Char](stream.input, env)).reset(3.events).all()
    val expected = data.grouped(3).map( generateAppendScan(_) ).reduce( _ ++ _ )
    new StreamTest("scan", expected, out)
  }

  test("MFunc grouped reduce") {
    val out = impl.streamOf2(new OldStyleFuncAppend[Char](stream.input, env)).reset(3.events).last()
    val expected = data.grouped(3).map( generateAppendScan(_).last ).toSeq
    new StreamTest("reduce", expected, out)
  }

// -------------- tricky composition of self-generator and binding
  test("MFunc bind scan") {
    val alternate:Function1[Char, Boolean] = new Function1[Char,Boolean] {
      var accept = false
      override def apply(v1: Char): Boolean = { accept = !accept; accept }
    }
    val alternateUppers = impl.asStream(stream.input).filter( alternate ).map( _.toUpper )
    val out = impl.streamOf2(new OldStyleFuncAppend[Char](stream.input, env)).bind(alternateUppers)(_.append).all()
    val expected = List(
      "Aa",
      "Aab",
      "AabCc",
      "AabCcd",
      "AabCcdEe",
      "AabCcdEef",
      "AabCcdEefGg",
      "AabCcdEefGgh",
      "AabCcdEefGghIi",
      "AabCcdEefGghIij",
      "AabCcdEefGghIijKk"
    ).map(_.toCharArray.toSeq)

    new StreamTest("scan", expected, out)
  }

  test("MFunc bind reduce") {
    val alternate:Function1[Char, Boolean] = new Function1[Char,Boolean] {
      var accept = false
      override def apply(v1: Char): Boolean = { accept = !accept; accept }
    }
    val alternateUppers = impl.asStream(stream.input).filter( alternate ).map( _.toUpper )
    val out = impl.streamOf2(new OldStyleFuncAppend[Char](stream.input, env)).bind(alternateUppers)(_.append).last()
    val expected = List("AabCcdEefGghIijKk".toCharArray.toSeq)
    new StreamTest("scan", expected, out)
  }

  test("MFunc bind grouped scan") {
    val alternate:Function1[Char, Boolean] = new Function1[Char,Boolean] {
      var accept = false
      override def apply(v1: Char): Boolean = { accept = !accept; accept }
    }
    val alternateUppers = impl.asStream(stream.input).filter( alternate ).map( _.toUpper )
    val out = impl.streamOf2(new OldStyleFuncAppend[Char](stream.input, env)).bind(alternateUppers)(_.append).reset(3.events).all()
    val expected = List(
      "Aa",
      "Aab",
      "AabCc",
           "d",
           "dEe",
           "dEef",
               "Gg",
               "Ggh",
               "GghIi",
                    "j",
                    "jKk"
    ).map(_.toCharArray.toSeq)

    new StreamTest("scan", expected, out)
  }

  test("MFunc bind grouped reduce") {
    val alternate:Function1[Char, Boolean] = new Function1[Char,Boolean] {
      var accept = false
      override def apply(v1: Char): Boolean = { accept = !accept; accept }
    }
    val alternateUppers = impl.asStream(stream.input).filter( alternate ).map( _.toUpper )
    val out = impl.streamOf2(new OldStyleFuncAppend[Char](stream.input, env)).bind(alternateUppers)(_.append).reset(3.events).last()
    val expected = List(
      "AabCc",
           "dEef",
               "GghIi",
                    "jKk"
    ).map(_.toCharArray.toSeq)

    new StreamTest("scan", expected, out)
  }
}
